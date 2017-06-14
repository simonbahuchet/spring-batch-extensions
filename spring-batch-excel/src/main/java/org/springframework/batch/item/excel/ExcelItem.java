package org.springframework.batch.item.excel;

/**
 * DTO to return both sheet and rowset/item
 */
public class ExcelItem<T> {

    private T item;

    private Sheet sheet;

    /**
     * Default constructor
     */
    ExcelItem() {

    }

    /**
     * Constructor
     * @param sheet
     * @param item
     */
    ExcelItem(Sheet sheet, T item) {
        this.sheet = sheet;
        this.item = item;
    }

    public T getItem() {
        return item;
    }

    public void setItem(T item) {
        this.item = item;
    }

    public Sheet getSheet() {
        return sheet;
    }

    public void setSheet(Sheet sheet) {
        this.sheet = sheet;
    }
}
