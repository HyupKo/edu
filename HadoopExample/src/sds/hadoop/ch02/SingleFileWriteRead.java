package sds.hadoop.ch02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SingleFileWriteRead {
  public static void main(String[] args) {
    // �Է� �Ķ���� Ȯ��
    if (args.length != 2) {
      System.err
          .println("Usage: SingleFileWriteRead <filename> <contents>");
      System.exit(2);
    }

    try {
      // ���� �ý��� ���� ��ü ����
      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(conf);

      // ��� üũ
      Path path = new Path(args[0]);
      if (hdfs.exists(path)) {
        hdfs.delete(path, true);
      }

      // ���� ����
      FSDataOutputStream outStream = hdfs.create(path);
      outStream.writeUTF(args[1]);
      outStream.close();

      // ���� ���
      FSDataInputStream inputStream = hdfs.open(path);
      String inputString = inputStream.readUTF();
      inputStream.close();

      System.out.println("## inputString:" + inputString);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}