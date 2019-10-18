package memstore.table;

import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * RowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 */
public class RowTable implements Table {
    protected int numCols; // 列数
    protected int numRows; // 行数
    protected ByteBuffer rows;

    public RowTable() { }

    public RowTable(int numCols, int numRows, ByteBuffer rows) {
        this.numCols = numCols;
        this.numRows = numRows;
        this.rows = rows;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException { // 从DataLoader对象里面 按行优先 取出数据放入rows
        this.numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols); // 分配内存

        for (int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId); // 取出一行
            for (int colId = 0; colId < numCols; colId++) { // 取出某一格
                int offset = ByteFormat.FIELD_LEN * ((rowId * numCols) + colId); // 偏移量
                this.rows.putInt(offset, curRow.getInt(ByteFormat.FIELD_LEN * colId)); // 根据index（offset）放入value
            }
        }
    }

    /**
     * Returns the int field at row `rowId` and column `colId`.
     */
    @Override
    public int getIntField(int rowId, int colId) {
        int offset = ByteFormat.FIELD_LEN * (rowId * numCols + colId);
        return rows.getInt(offset);
    }

    /**
     * Inserts the passed-in int field at row `rowId` and column `colId`.
     */
    @Override
    public void putIntField(int rowId, int colId, int field) {
        int offset = ByteFormat.FIELD_LEN * (rowId * numCols + colId);
        rows.putInt(offset, field);
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table;
     *
     *  Returns the sum of all elements in the first column of the table.
     */
    @Override
    public long columnSum() {
        long sum = 0;
        for(int i = 0; i < numRows; i++) {
            int offset = i * ByteFormat.FIELD_LEN * numCols;
            sum += rows.getInt(offset);
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
     *
     *  Returns the sum of all elements in the first column of the table,
     *  subject to the passed-in predicates.
     */
    @Override
    public long predicatedColumnSum(int threshold1, int threshold2) {
        long sum = 0;
        for(int i = 0; i < numRows; i++) {
            int offset = i * ByteFormat.FIELD_LEN * numCols;
            int col1 = rows.getInt(offset + ByteFormat.FIELD_LEN);
            int col2 = rows.getInt(offset + ByteFormat.FIELD_LEN * 2);
            if(col1 > threshold1 && col2 < threshold2) {
                sum += rows.getInt(offset);
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *  SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 > threshold;
     *
     *  Returns the sum of all elements in the rows which pass the predicate.
     */
    @Override
    public long predicatedAllColumnsSum(int threshold) {
        long sum = 0;
        for(int i = 0; i < numRows; i++) {
            int offset = i * ByteFormat.FIELD_LEN * numCols;
            if(rows.getInt(offset) > threshold) {
                for(int j = 0; j < numCols; j++) {
                    sum += rows.getInt(offset + j * ByteFormat.FIELD_LEN);
                }
            }
        }
        return sum;
    }

    /**
     * Implements the query
     *   UPDATE(col3 = col1 + col2) WHERE col0 < threshold;
     *
     *   Returns the number of rows updated.
     */
    @Override
    public int predicatedUpdate(int threshold) {
        int count = 0;
        for(int i = 0; i < numRows; i++) {
            int offset = i * ByteFormat.FIELD_LEN * numCols;
            if(rows.getInt(offset) < threshold) {
                count++;
                rows.putInt(offset + ByteFormat.FIELD_LEN * 3,
                        rows.getInt(offset + ByteFormat.FIELD_LEN * 2)
                                + rows.getInt(offset + ByteFormat.FIELD_LEN));
            }
        }
        return count;
    }
}
