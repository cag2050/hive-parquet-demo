import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * 解析Parquet文件内容：https://blog.csdn.net/baidu_32377671/article/details/117253718
 * java读取parquet文件 附带int32(date)和int96(timestamp)转换: https://blog.csdn.net/weixin_44385419/article/details/115671140
 */
public class ReadParquetDemo {
  private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
  private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
  private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

  public static void javaReadParquet(String path) throws IOException {
    GroupReadSupport readSupport = new GroupReadSupport();
    ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(path));
    ParquetReader<Group> build = reader.build();
    Group line;
    while ((line = build.read()) != null) {
      Binary col0Binary = line.getInt96("_col0", 0);
      System.out.println("Binary: " + col0Binary);
      System.out.println("时间戳: " + getTimestampMillis(col0Binary));
      System.out.println("时间字符串:" + getDatetimeStr(getTimestampMillis(col0Binary)));
      System.out.println("====");

      Binary col1Binary = line.getInt96("_col1", 0);
      System.out.println("Binary: " + col1Binary);
      System.out.println("时间戳: " + getTimestampMillis(col1Binary));
      System.out.println("时间字符串:" + getDatetimeStr(getTimestampMillis(col1Binary)));
      System.out.println("====");
    }
    build.close();
  }

  private static long julianDayToMillis(int julianDay) {
    return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
  }

  public static long getTimestampMillis(Binary timestampBinary) {
    if (timestampBinary.length() != 12) {
      throw new RuntimeException("HIVE_BAD_DATA: Parquet timestamp must be 12 bytes, actual " + timestampBinary.length());
    }
    byte[] bytes = timestampBinary.getBytes();

    // little endian encoding - need to invert byte order
    long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
    int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

    return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
  }

  public static String getDatetimeStr(long timestampMillis) {
    System.out.println(ZoneId.systemDefault());
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis), ZoneId.of("GMT+08:00"));
//    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis), ZoneId.of("GMT"));
    return zonedDateTime.format(formatter);
  }


  public static void main(String[] args) {
    try {
//      javaReadParquet("/Users/cag2050/Documents/IdeaProjects4Paradigm/hive-parquet-demo/src/main/java/172.27.128.190_000000_0.gz.parquet");
//      javaReadParquet("/Users/cag2050/Documents/IdeaProjects4Paradigm/hive-parquet-demo/src/main/java/172.27.128.190_null_000000_0.gz.parquet");
      javaReadParquet("/Users/cag2050/Documents/IdeaProjects4Paradigm/hive-parquet-demo/src/main/java/172.27.231.51_000000_0.gz.parquet");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
