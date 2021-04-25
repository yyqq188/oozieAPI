package com.pactera.oozieAPI.json2sql.utils;

import com.alibaba.excel.event.NotRepeatExecutor;
import com.alibaba.excel.metadata.CellData;
import com.alibaba.excel.metadata.Head;
import com.alibaba.excel.write.handler.CellWriteHandler;
import com.alibaba.excel.write.handler.WorkbookWriteHandler;
import com.alibaba.excel.write.metadata.holder.WriteSheetHolder;
import com.alibaba.excel.write.metadata.holder.WriteTableHolder;
import com.alibaba.excel.write.metadata.holder.WriteWorkbookHolder;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;

import java.util.List;

public abstract class CustomAbstractCellStyleStrategy implements CellWriteHandler, WorkbookWriteHandler, NotRepeatExecutor {
    boolean hasInitialized = false;

    public CustomAbstractCellStyleStrategy() {
    }

    public String uniqueValue() {
        return "CellStyleStrategy";
    }

    public void beforeCellCreate(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, Row row, Head head, Integer columnIndex, Integer relativeRowIndex, Boolean isHead) {
    }

    public void afterCellCreate(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, Cell cell, Head head, Integer relativeRowIndex, Boolean isHead) {
    }

    public void afterCellDataConverted(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, CellData cellData, Cell cell, Head head, Integer relativeRowIndex, Boolean isHead) {
    }

    public void afterCellDispose(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, List<CellData> cellDataList, Cell cell, Head head, Integer relativeRowIndex, Boolean isHead) {
        if (isHead != null) {
            if (isHead) {
                this.setHeadCellStyle(writeSheetHolder,writeTableHolder,cellDataList,cell, head, relativeRowIndex);
            } else {
                this.setContentCellStyle(writeSheetHolder,writeTableHolder,cellDataList,cell, head, relativeRowIndex);
            }

        }
    }

    public void beforeWorkbookCreate() {
    }

    public void afterWorkbookCreate(WriteWorkbookHolder writeWorkbookHolder) {
        this.initCellStyle(writeWorkbookHolder.getWorkbook());
        this.hasInitialized = true;
    }

    public void afterWorkbookDispose(WriteWorkbookHolder writeWorkbookHolder) {
    }

    protected abstract void initCellStyle(Workbook var1);

    protected abstract void setHeadCellStyle(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, List<CellData> cellDataList,Cell var1, Head var2, Integer var3);

    protected abstract void setContentCellStyle(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, List<CellData> cellDataList,Cell var1, Head var2, Integer var3);
}
