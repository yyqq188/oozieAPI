package com.pactera.oozieAPI.json2sql.utils;

import com.alibaba.excel.enums.CellDataTypeEnum;
import com.alibaba.excel.metadata.CellData;
import com.alibaba.excel.metadata.Head;
import com.alibaba.excel.util.CollectionUtils;
import com.alibaba.excel.write.metadata.holder.WriteSheetHolder;
import com.alibaba.excel.write.metadata.holder.WriteTableHolder;
import org.apache.poi.ss.usermodel.*;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomExcelStyleHandler  extends CustomAbstractCellStyleStrategy {
    private CellStyle headCellStyle;
    private List<CellStyle> contentCellStyleList;

    private static final int MAX_COLUMN_WIDTH = 255;
    private  Map<Integer, Map<Integer, Integer>> CACHE = new HashMap(8);

    public static String DATE_FORMAT_01 = "yyyy/MM/dd HH:mm:ss";//日期格式1 ,Excel默认日期格式
    public static String DATE_FORMAT_02 = "yyyy/MM/dd";//日期格式2
    public static String DATE_FORMAT_03 = "yyyy-MM-dd HH:mm:ss";//日期格式3
    public static String DATE_FORMAT_04 = "yyyy-MM-dd";//日期格式4
    public static String DATE_PATTERN_01 ="\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}";//日期格式校验1
    public static String DATE_PATTERN_02 ="\\d{4}/\\d{2}/\\d{2}";//日期格式校验2
    public static String DATE_PATTERN_03 ="\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";//日期格式校验3
    public static String DATE_PATTERN_04 ="\\d{4}-\\d{2}-\\d{2}";//日期格式校验4

    public CustomExcelStyleHandler() {
    }

    public CustomExcelStyleHandler(CellStyle headWriteCellStyle, List<CellStyle> contentWriteCellStyleList) {
        this.headCellStyle = headWriteCellStyle;
        this.contentCellStyleList = contentWriteCellStyleList;
    }

    protected void initCellStyle(Workbook workbook) {

    }

    /**
     * 设置标题样式
     */
    protected void setHeadCellStyle(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, List<CellData> cellDataList, Cell cell, Head head, Integer relativeRowIndex) {
        if (this.headCellStyle == null) {
            this.headCellStyle = getDefaultHeadStyle(cell.getSheet().getWorkbook());
        }
        if(head.getHeadNameList().size()==3&&relativeRowIndex==1){//第二行左对齐
            CellStyle mCellStyle = getDefaultHeadStyle(cell.getSheet().getWorkbook());
            // 水平对齐方式,左中右
            mCellStyle.setAlignment(HorizontalAlignment.LEFT);
            // 垂直对齐方式，上中下
            mCellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
            cell.setCellStyle(mCellStyle);
        }else{
            cell.setCellStyle(this.headCellStyle);
        }
        setColumnWidth(writeSheetHolder,cellDataList,cell,head,relativeRowIndex,true);
    }
    /**
     * 设置内容样式
     */
    protected void setContentCellStyle(WriteSheetHolder writeSheetHolder, WriteTableHolder writeTableHolder, List<CellData> cellDataList,Cell cell, Head head, Integer relativeRowIndex) {
        int columnIndex = head.getColumnIndex();
        CellStyle cellStyle = null;
        if(contentCellStyleList==null||(columnIndex+1)>contentCellStyleList.size()){
            if(contentCellStyleList==null){
                contentCellStyleList = new ArrayList<>();
            }
            Workbook workbook = cell.getSheet().getWorkbook();
            cellStyle = getDefalutContentStyle(workbook);
            DataFormat dataFormat = workbook.createDataFormat();
            CellData cellData = cellDataList.get(0);
            CellDataTypeEnum type = cellData.getType();
            switch(type) {
                case STRING:
                    String stringValue =  cellData.getStringValue();
                    if(StringUtils.isInteger(stringValue)){
                        cell.setCellValue(Integer.parseInt(stringValue));
                    }else{
                        checkDateStyle(cell,cellStyle);//判断数据是否符合日期格式，并设置对应的日期格式
                    }
                    break;
                case NUMBER:
                    BigDecimal numberValue = cellData.getNumberValue();
                    if(numberValue==null|| StringUtils.isInteger(numberValue.toString())||numberValue.compareTo(BigDecimal.ZERO)==0){//整形
                        cellStyle.setDataFormat(dataFormat.getFormat("0"));
                    }else {//浮点型
                        cellStyle.setDataFormat(dataFormat.getFormat("0.00"));//默认保留两位小数
                    }
                    break;
                default:
                    break;
            }
            contentCellStyleList.add(cellStyle);
        }else{
            cellStyle = contentCellStyleList.get(columnIndex);
        }
        cell.setCellStyle(cellStyle);
        setColumnWidth(writeSheetHolder,cellDataList,cell,head,relativeRowIndex,false);
    }
    /**
     * 标题样式
     * @param workbook workbook
     * @return CellStyle
     */
    public CellStyle getDefaultHeadStyle(Workbook workbook){

        CellStyle cellStyle = workbook.createCellStyle();
        // 下边框
        cellStyle.setBorderBottom(BorderStyle.THIN);
        // 左边框
        cellStyle.setBorderLeft(BorderStyle.THIN);
        // 上边框
        cellStyle.setBorderTop(BorderStyle.THIN);
        // 右边框
        cellStyle.setBorderRight(BorderStyle.THIN);
        // 水平对齐方式
        cellStyle.setAlignment(HorizontalAlignment.CENTER);
        // 垂直对齐方式
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        Font font = workbook.createFont();
        font.setFontName("宋体");
        font.setFontHeightInPoints((short)12);//设置字体大小
        font.setBold(true);//字体加粗
        cellStyle.setFont(font);
        return cellStyle;
    }

    /**
     * 内容样式
     */
    private CellStyle getDefalutContentStyle(Workbook workbook) {
        CellStyle cellStyle = workbook.createCellStyle();
        // 下边框
        cellStyle.setBorderBottom(BorderStyle.THIN);
        // 左边框
        cellStyle.setBorderLeft(BorderStyle.THIN);
        // 上边框
        cellStyle.setBorderTop(BorderStyle.THIN);
        // 右边框
        cellStyle.setBorderRight(BorderStyle.THIN);
        // 水平对齐方式
        cellStyle.setAlignment(HorizontalAlignment.CENTER);
        // 垂直对齐方式
        cellStyle.setVerticalAlignment(VerticalAlignment.CENTER);
        Font font = workbook.createFont();
        font.setFontName("宋体");
        font.setFontHeightInPoints((short)11);
        cellStyle.setFont(font);
        return cellStyle;
    }

    /**
     * 设置列宽
     */
    private void setColumnWidth(WriteSheetHolder writeSheetHolder, List<CellData> cellDataList, Cell cell, Head head, Integer relativeRowIndex, Boolean isHead) {
        //指定宽度条件，根据head最后一行和数据行前20行设置列宽
        boolean needSetWidth = (isHead&&cell.getStringCellValue().equals(head.getHeadNameList().get(head.getHeadNameList().size()-1)))
                ||(relativeRowIndex<20&&!CollectionUtils.isEmpty(cellDataList)&& StringUtils.isNotEmpty(cellDataList.get(0))) ;
        if (needSetWidth) {
            Map<Integer, Integer> maxColumnWidthMap = CACHE.get(writeSheetHolder.getSheetNo());
            if (maxColumnWidthMap == null) {
                maxColumnWidthMap = new HashMap(16);
                CACHE.put(writeSheetHolder.getSheetNo(), maxColumnWidthMap);
            }

            Integer columnWidth = this.dataLength(cellDataList, cell, isHead);
            if (columnWidth >= 0) {
                if (columnWidth > MAX_COLUMN_WIDTH) {//不能大于最大列宽
                    columnWidth = MAX_COLUMN_WIDTH;
                }

                Integer maxColumnWidth = (Integer)((Map)maxColumnWidthMap).get(cell.getColumnIndex());
                if (maxColumnWidth == null || columnWidth > maxColumnWidth) {
                    ((Map)maxColumnWidthMap).put(cell.getColumnIndex(), columnWidth);
                    writeSheetHolder.getSheet().setColumnWidth(cell.getColumnIndex(), columnWidth * 256);
//                    writeSheetHolder.getSheet().setDefaultColumnStyle(, );
                }

            }
        }
    }

    /**
     * 表格数据长度
     * @param cellDataList 表格数据
     * @param cell 表格
     * @param isHead 是否标题
     * @return 长度
     */
    private Integer dataLength(List<CellData> cellDataList, Cell cell, Boolean isHead) {
        if (isHead) {
            return getCharRealCountHeader(cell.getStringCellValue());
//            return cell.getStringCellValue().getBytes().length;
        } else {
            CellData cellData = cellDataList.get(0);
            CellDataTypeEnum type = cellData.getType();
            if (type == null) {
                return -1;
            } else {
                switch(type) {
                    case STRING:
                        return getCharRealCount(cellData.getStringValue());
                    case BOOLEAN:
                        return cellData.getBooleanValue().toString().getBytes().length;
                    case NUMBER:
                        return cellData.getNumberValue().toString().getBytes().length+2;//number类型占用长度微调
                    default:
                        return -1;
                }
            }
        }
    }

    /**
     * 获取字符串占用真实长度
     * @param str 字符
     * @return 长度
     */
    private Integer getCharRealCountHeader(String str) {
        double count = 0;
        if(StringUtils.isNotEmpty(str)) {
            for (char c : str.toCharArray()) {
                if (StringUtils.isChinese(c)) count = count + 3;//中文
                else if (StringUtils.isEnglish(c)) count = count + 1.5;//英文
                else if (Character.isDigit(c)) count = count + 1;//数字
                else count = count+1.5;//其他特殊字符
            }
        }
        return (int) count + 2;//取整加1
    }
    private Integer getCharRealCount(String str) {
        double count = 0;
        if(StringUtils.isNotEmpty(str)) {
            for (char c : str.toCharArray()) {
                if (StringUtils.isChinese(c)) count = count + 2;//中文
                else if (StringUtils.isEnglish(c)) count = count + 1;//英文
                else if (Character.isDigit(c)) count = count + 1;//数字
                else count = count+1;//其他特殊字符
            }
        }
        return (int) count + 1;//取整加1
    }

    /**
     * 校验日期格式
     * @param cell
     * @return
     */
    private boolean checkDateStyle(Cell cell,CellStyle cellStyle) {
        //Excel存储日期、时间均以数值类型进行存储，读取时POI先判断是是否是数值类型，再进行判断
        boolean isDate = false;
        DataFormat dataFormat = cell.getSheet().getWorkbook().createDataFormat();
        if (CellType.NUMERIC == cell.getCellTypeEnum()){
            //如果是日期格式
            if(DateUtil.isCellDateFormatted(cell)){
                isDate = true;
                cellStyle.setDataFormat(dataFormat.getFormat(DATE_FORMAT_01));
            }
        }else if(CellType.STRING == cell.getCellTypeEnum()){
            String stringCellValue = cell.getStringCellValue();
            try {
                if(StringUtils.isNotEmpty(stringCellValue)&&stringCellValue.length()==DATE_FORMAT_01.length()&&stringCellValue.matches(DATE_PATTERN_01)) {
                    isDate = true;
                    cell.setCellValue(new SimpleDateFormat(DATE_FORMAT_01).parse(stringCellValue));
                    cellStyle.setDataFormat(dataFormat.getFormat(DATE_FORMAT_01));
                }
                if(StringUtils.isNotEmpty(stringCellValue)&&stringCellValue.length()==DATE_FORMAT_02.length()&&stringCellValue.matches(DATE_PATTERN_02)) {
                    isDate = true;
                    cell.setCellValue(new SimpleDateFormat(DATE_FORMAT_02).parse(stringCellValue));
                    cellStyle.setDataFormat(dataFormat.getFormat(DATE_FORMAT_02));
                }
                if(StringUtils.isNotEmpty(stringCellValue)&&stringCellValue.length()==DATE_FORMAT_03.length()&&stringCellValue.matches(DATE_PATTERN_03)) {
                    isDate = true;
                    cell.setCellValue(new SimpleDateFormat(DATE_FORMAT_03).parse(stringCellValue));
                    cellStyle.setDataFormat(dataFormat.getFormat(DATE_FORMAT_03));
                }
                if(StringUtils.isNotEmpty(stringCellValue)&&stringCellValue.length()==DATE_FORMAT_04.length()&&stringCellValue.matches(DATE_PATTERN_04)) {
                    isDate = true;
                    cell.setCellValue(new SimpleDateFormat(DATE_FORMAT_04).parse(stringCellValue));
                    cellStyle.setDataFormat(dataFormat.getFormat(DATE_FORMAT_04));
                }
            } catch (Exception e) {
                //不做操作
            }
        }
        return isDate;
    }
}
