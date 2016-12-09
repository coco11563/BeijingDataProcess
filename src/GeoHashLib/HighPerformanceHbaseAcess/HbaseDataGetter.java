package GeoHashLib.HighPerformanceHbaseAcess;

import GeoHashLib.Hbase.CheckinDAO;
import io.netty.buffer.ByteBuf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.apache.hadoop.hbase.test.IntegrationTestBigLinkedListWithVisibility.tableName;

/**
 * Created by root on 12/9/16.
 *
 */
class HbaseDataGetter implements Callable<List<Data>>
{
    private List<String> rowKeys;
    private List<String> filterColumn;
    private boolean isContiansRowkeys;
    private boolean isContainsList;

    public HbaseDataGetter(List<String> rowKeys, List<String> filterColumn,
                           boolean isContiansRowkeys, boolean isContainsList)
    {
        this.rowKeys = rowKeys;
        this.filterColumn = filterColumn;
        this.isContiansRowkeys = isContiansRowkeys;
        this.isContainsList = isContainsList;
    }

    @Override
    public List<Data> call() throws Exception
    {
        Object[] objects = getDatasFromHbase(rowKeys, filterColumn);
        List<Data> listData = new ArrayList<Data>();
        for (Object object : objects)
        {
            Result r = (Result) object;
            Data data = assembleData(r, filterColumn, isContiansRowkeys,
                    isContainsList);
            listData.add(data);
        }
        return listData;
    }

    private Data assembleData(Result r, List<String> filterColumn, boolean isContiansRowkeys, boolean isContainsList) {
        return null;
    }

    private ExecutorService pool = Executors.newFixedThreadPool(10);    // 这里创建了10个 Active RPC Calls
    public Datas getDatasFromHbase(final List<String> rowKeys,
                                   final List<String> filterColumn, boolean isContiansRowkeys,
                                   boolean isContainsList) {
        if (rowKeys == null || rowKeys.size() <= 0)
        {
            return Datas.getEmptyDatas();
        }
        final int maxRowKeySize = 1000;
        int loopSize = rowKeys.size() % maxRowKeySize == 0 ? rowKeys.size()
                / maxRowKeySize : rowKeys.size() / maxRowKeySize + 1;
        ArrayList<Future<List<Data>>> results = new ArrayList<Future<List<Data>>>();
        for (int loop = 0; loop < loopSize; loop++)
        {
            int end = (loop + 1) * maxRowKeySize > rowKeys.size() ? rowKeys
                    .size() : (loop + 1) * maxRowKeySize;
            List<String> partRowKeys = rowKeys.subList(loop * maxRowKeySize,
                    end);
            HbaseDataGetter hbaseDataGetter = new HbaseDataGetter(partRowKeys,
                    filterColumn, isContiansRowkeys, isContainsList);
            synchronized (pool)
            {
                Future<List<Data>> result = pool.submit(hbaseDataGetter);
                results.add(result);
            }
        }
        Datas datas = new Datas();
        List<Data> dataQueue = new ArrayList<Data>();
        try
        {
            for (Future<List<Data>> result : results)
            {
                List<Data> rd = result.get();
                dataQueue.addAll(rd);
            }
            datas.setDatas(dataQueue);
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace();
        }
        return datas;
    }
    private Object[] getDatasFromHbase(List<String> rowKeys, List<String> filterColumn) {
        {
            createTable(tableName);
            Object[] objects = null;
            HTableInterface hTableInterface = createTable(tableName);
            List<Get> listGets = new ArrayList<Get>();
            for (String rk : rowKeys)
            {
                Get get = new Get(Bytes.toBytes(rk));
                if (filterColumn != null)
                {
                    for (String column : filterColumn)
                    {
                        get.addColumn(CheckinDAO.FAMILY_NAME,
                                column.getBytes());
                    }
                }
                listGets.add(get);
            }
            try
            {
                objects = hTableInterface.get(listGets);
            }
            catch (IOException e1)
            {
                e1.printStackTrace();
            }
            finally
            {
                try
                {
                    listGets.clear();
                    hTableInterface.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            return objects;
        }
    }

    private HTableInterface createTable(String tableName)
    {
        HTable table = null;
        try
        {
            table = new HTable(initHbaseConfiguration(), tableName);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return table;
    }

    private Configuration initHbaseConfiguration() {
        return HBaseConfiguration.create();
    }
}
