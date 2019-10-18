package memstore.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import javafx.util.Pair;
import memstore.data.ByteFormat;
import memstore.data.DataLoader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * IndexedRowTable, which stores data in row-major format.
 * That is, data is laid out like
 *   row 1 | row 2 | ... | row n.
 *
 * Also has a tree index on column `indexColumn`, which points
 * to all row indices with the given value.
 */
public class IndexedRowTable implements Table {

    int numCols;
    int numRows;
    private TreeMap<Integer, IntArrayList> index;
    private ByteBuffer rows;
    private int indexColumn;
    private RowTable rowTable;

    /**
     * 用户可调用此函数以设置哪一列为 indexColumn
     * @param indexColumn
     */
    public IndexedRowTable(int indexColumn) {
        this.indexColumn = indexColumn;
    }

    /**
     * Loads data into the table through passed-in data loader. Is not timed.
     *
     * @param loader Loader to load data from.
     * @throws IOException
     */
    @Override
    public void load(DataLoader loader) throws IOException {
        numCols = loader.getNumCols();
        List<ByteBuffer> rows = loader.getRows();
        numRows = rows.size();
        this.rows = ByteBuffer.allocate(ByteFormat.FIELD_LEN * numRows * numCols);
        index = new TreeMap<>();

        for(int rowId = 0; rowId < numRows; rowId++) {
            ByteBuffer curRow = rows.get(rowId);
            for(int colId = 0; colId < numCols; colId++) {
                int offset = ByteFormat.FIELD_LEN * (rowId * numCols + colId);
                int value = curRow.getInt(ByteFormat.FIELD_LEN * colId);
                this.rows.putInt(offset, value);
                // 下面开始建立索引
                if(colId == indexColumn) {
                    if(!index.containsKey(value)) {
                        index.put(value, new IntArrayList());
                    }
                    index.get(value).add(rowId);
                }
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
        /**
         * 鉴于index的结构是<Integer, IntArrayList>
         * 其中index.key是int型 保存的是数值；index.value是int数组 保存的是该数值所存在的行
         * index结构为{key : values} 即 {key : [value1, value2, ...]}
         */
        long sum = 0;
        if(indexColumn == 0) { // 如果索引列正好是col0，那就可以使用红黑树index来快速检索
            for(int key : index.keySet()) { // 用key来遍历index，注意这里的key在this.rows中的含义是在field中保存的数值
                IntArrayList values = index.get(key); // 获取保存了值为key的field所在的行 用行号（rowId）构成的数组
                sum += key * values.size(); // 数值*数值出现过在哪几行 = 这个数值的总和
            }
        } else { // 索引不是col0，只能用传统的行优先时的方式
            for(int i = 0; i < numRows; i++) {
                int offset = i * ByteFormat.FIELD_LEN * numCols;
                sum += rows.getInt(offset);
            }
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
        rowTable = new RowTable(numCols, numRows, rows);
        long sum = 0;
        switch (indexColumn) {
            case 1:
            case 2:
            case 0:
            default:
                sum = rowTable.predicatedColumnSum(threshold1, threshold2);
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
        rowTable = new RowTable(numCols, numRows, rows);
        long sum = 0;
        sum = rowTable.predicatedAllColumnsSum(threshold);
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
        rowTable = new RowTable(numCols, numRows, rows);
        int count = 0;
        count = rowTable.predicatedUpdate(threshold);
        return count;
    }
}
