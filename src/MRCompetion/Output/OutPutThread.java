package MRCompetion.Output;

import java.io.*;

import static MRCompetion.Output.CheckOutPutThread.COPTBQ;

/**
 * Created by coco1 on 2016/11/14.
 *
 * 导出口
 */
public class OutPutThread extends Thread{
    private final static File outputpath =  new File("C:\\Users\\coco1\\IdeaProjects\\BeijingDataProcess\\OutputData\\out");
    @Override
    public void run() {
        assert checkExist(outputpath);
        int i = 0;
        BufferedOutputStream bufferedOutputStream =
                null;
        try {
            bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(outputpath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while (i < 100) {
            byte[] data = new byte[0];
            try {
                data = COPTBQ.take().toString().getBytes("UTF-8");
                i = 0;
                assert bufferedOutputStream != null;
                bufferedOutputStream.write(data);
            } catch (InterruptedException e) {
                i ++;
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            assert bufferedOutputStream != null;
            bufferedOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private boolean checkExist(File file1) {
        if (file1.exists()) {
            System.out.println("存在文件夹a");
            return true;
        } else {
            return file1.mkdir(); // 文件夹的创建 创建文件夹/home/a123/a
        }
    }
}
