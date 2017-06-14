# slightly modified spring-batch-excel

Please see https://github.com/spring-projects/spring-batch-extensions/tree/master/spring-batch-excel

I needed to skip some sheets. That's why I
 added these files:
  - DefaultRowMapper.java
  - ExcelItem.java
  - SheetCallbackHandler.java

 and modified those ones:
  - AbstractExcelItemReader.java
  - RowCallbackHandler.java
  - RowMapper.java
  - PoiItemReader.java