package org.springframework.batch.item.excel;

import org.springframework.batch.item.excel.support.rowset.RowSet;

import java.util.HashMap;
import java.util.Map;

/**
 * Returns the Sheet + RowSet to let the processor access the row index
 */
public class DefaultRowMapper implements RowMapper<ExcelItem> {

    @Override
    public ExcelItem mapRow(final Sheet sheet, RowSet rs) throws Exception {
        Map result = new HashMap();
        result.put("currentRowIndex", rs.getCurrentRowIndex());
        result.put("currentRow", rs.getCurrentRow());
        return new ExcelItem(sheet, result);
    }
}
