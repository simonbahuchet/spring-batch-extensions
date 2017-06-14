/*
 * Copyright 2006-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.item.excel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.excel.support.rowset.DefaultRowSetFactory;
import org.springframework.batch.item.excel.support.rowset.RowSet;
import org.springframework.batch.item.excel.support.rowset.RowSetFactory;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.List;

/**
 * {@link org.springframework.batch.item.ItemReader} implementation to read an Excel
 * file. It will read the file sheet for sheet and row for row. It is loosy based on
 * the {@link org.springframework.batch.item.file.FlatFileItemReader}
 *
 * @param <T> the type
 * @author Marten Deinum
 * @since 0.5.0
 */
public abstract class AbstractExcelItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements
        ResourceAwareItemReaderItemStream<T>, InitializingBean {

    protected final Log logger = LogFactory.getLog(getClass());
    private Resource resource;
    private int linesToSkip = 0;
    private Collection<String> sheetsToSkip;
    private int currentSheet = 0;
    private RowMapper<T> rowMapper;
    private RowCallbackHandler skippedRowsCallback;
    private SheetCallbackHandler skippedSheetsCallbackHandler;
    private boolean strict = true;
    private RowSetFactory rowSetFactory = new DefaultRowSetFactory();
    private RowSet rs;

    public AbstractExcelItemReader() {
        super();
        this.setName(ClassUtils.getShortName(this.getClass()));
    }

    /**
     * @return string corresponding to logical record according to
     * {@link #setRowMapper(RowMapper)} (might span multiple rows in file).
     */
    @Override
    protected T doRead() throws Exception {

        if (this.rs == null) {
            return null;
        }

        if (rs.next()) {
            try {
                return this.rowMapper.mapRow(this.getSheet(this.currentSheet), rs);
            } catch (final Exception e) {
                throw new ExcelFileParseException("Exception parsing Excel file.", e, this.resource.getDescription(),
                        rs.getMetaData().getSheetName(), rs.getCurrentRowIndex(), rs.getCurrentRow());
            }
        } else {

            // We read the last row of the current sheet. Let's pass to the next sheet, if any
            this.currentSheet++;

            if (this.nextSheet()) {
                return this.doRead();
            }
            return null;
        }
    }

    @Override
    protected void doOpen() throws Exception {
        Assert.notNull(this.resource, "Input resource must be set");
        if (!this.resource.exists()) {
            if (this.strict) {
                throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode): "
                        + this.resource);
            }
            logger.warn("Input resource does not exist '" + this.resource.getDescription() + "'.");
            return;
        }

        if (!this.resource.isReadable()) {
            if (this.strict) {
                throw new IllegalStateException("Input resource must be readable (reader is in 'strict' mode): "
                        + this.resource);
            }
            logger.warn("Input resource is not readable '" + this.resource.getDescription() + "'.");
            return;
        }

        this.openExcelFile(this.resource);
        this.nextSheet();
        if (logger.isDebugEnabled()) {
            logger.debug("Opened workbook [" + this.resource.getFilename() + "] with " + this.getNumberOfSheets() + " sheets.");
        }
    }

    /**
     * Go to the next available row:
     * - skip the sheets that need to be
     * - skipp the rows that need to be
     *
     * @return true if there's another data (row within a sheet) to read
     */
    private boolean nextSheet() {
        final Sheet sheet = getNextAvailableSheet();
        if (sheet == null) {
            return false;
        }

        this.rs = rowSetFactory.create(sheet);
        if (logger.isDebugEnabled()) {
            logger.debug("Opening sheet " + sheet.getName() + ".");
        }

        // Go to next row
        boolean nextRow = nextRow(sheet);

        if (logger.isDebugEnabled()) {
            logger.debug("Opened sheet " + sheet.getName() + ", with " + sheet.getNumberOfRows() + " rows.");
        }
        return nextRow;
    }

    /**
     * Go to next available row
     *
     * @param sheet only used for logs
     * @return true if there's at least one row to read (not skipped)
     */
    private boolean nextRow(Sheet sheet) {
        boolean noMoreRows = false;
        for (int i = 0; i < this.linesToSkip; i++) {
            if (rs.next()) {
                logger.debug(String.format("[%s] Skipping line %d", sheet.getName(), i));
                if (this.skippedRowsCallback != null) {
                    this.skippedRowsCallback.handleRow(sheet, rs);
                }
            } else {
                noMoreRows = true;
                break;
            }
        }
        if (noMoreRows) {
            logger.debug(String.format("[%s] All rows have been skipped", sheet.getName()));
            return false;
        }

        // There is at least one row within the sheet, that we can read
        return true;
    }

    /**
     * Iterate over the sheets until the last one. Skip the ones that are 'skippable'
     *
     * @return
     */
    Sheet getNextAvailableSheet() {
        if (this.currentSheet >= this.getNumberOfSheets()) {
            if (logger.isDebugEnabled()) {
                logger.debug("No more sheets in '" + this.resource.getDescription() + "'.");
            }
            return null;
        }

        final Sheet sheet = this.getSheet(this.currentSheet);

        if (!CollectionUtils.isEmpty(this.sheetsToSkip) && this.sheetsToSkip.contains(sheet.getName())) {
            logger.debug(String.format("Skipping sheet %s", sheet.getName()));
            if (skippedSheetsCallbackHandler != null) {
                skippedSheetsCallbackHandler.handleSheet(sheet);
            }
            this.currentSheet++;
            return getNextAvailableSheet();
        } else {
            return sheet;
        }
    }

    /**
     * Public setter for the input resource.
     *
     * @param resource the {@code Resource} pointing to the Excelfile
     */
    public void setResource(final Resource resource) {
        this.resource = resource;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(this.rowMapper, "RowMapper must be set");
    }

    /**
     * Set the number of lines to skip. This number is applied to all worksheet
     * in the excel file! default to 0
     *
     * @param linesToSkip number of lines to skip
     */
    public void setLinesToSkip(final int linesToSkip) {
        this.linesToSkip = linesToSkip;
    }

    /**
     * @param sheet the sheet index
     * @return the sheet or <code>null</code> when no sheet available.
     */
    protected abstract Sheet getSheet(int sheet);

    /**
     * The number of sheets in the underlying workbook.
     *
     * @return the number of sheets.
     */
    protected abstract int getNumberOfSheets();

    /**
     * @param resource {@code Resource} pointing to the Excel file to read
     * @throws Exception when the Excel sheet cannot be accessed
     */
    protected abstract void openExcelFile(Resource resource) throws Exception;

    /**
     * In strict mode the reader will throw an exception on
     * {@link #open(org.springframework.batch.item.ExecutionContext)} if the input resource does not exist.
     *
     * @param strict true by default
     */
    public void setStrict(final boolean strict) {
        this.strict = strict;
    }

    /**
     * Public setter for the {@code rowMapper}. Used to map a row read from the underlying Excel workbook.
     *
     * @param rowMapper the {@code RowMapper} to use.
     */
    public void setRowMapper(final RowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
    }

    /**
     * Public setter for the <code>rowSetFactory</code>. Used to create a {@code RowSet} implemenation. By default the
     * {@code DefaultRowSetFactory} is used.
     *
     * @param rowSetFactory the {@code RowSetFactory} to use.
     */
    public void setRowSetFactory(RowSetFactory rowSetFactory) {
        this.rowSetFactory = rowSetFactory;
    }

    /**
     * @param skippedRowsCallback will be called for each one of the initial skipped lines before any items are read.
     */
    public void setSkippedRowsCallback(final RowCallbackHandler skippedRowsCallback) {
        this.skippedRowsCallback = skippedRowsCallback;
    }

    /**
     * @param skippedRowsCallback will be called for each one of the initial skipped lines before any items are read.
     */
    public void setSkippedSheetsCallback(final RowCallbackHandler skippedRowsCallback) {
        this.skippedRowsCallback = skippedRowsCallback;
    }

    /**
     * @param sheetsToSkip name of the sheets to skip
     */
    public void setSheetsToSkip(List<String> sheetsToSkip) {
        this.sheetsToSkip = sheetsToSkip;
    }

    /**
     * @return the names of the sheets to skip
     */
    public Collection<String> getSheetsToSkip() {
        return sheetsToSkip;
    }

    /**
     * @param skippedSheetsCallbackHandler a handler that is called every time a sheet is skipped
     */
    public void setSkippedSheetsCallbackHandler(SheetCallbackHandler skippedSheetsCallbackHandler) {
        this.skippedSheetsCallbackHandler = skippedSheetsCallbackHandler;
    }
}
