package nestedcolumn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

/**
 * Created by root on 8/15/17.
 */
public class DataGen {
    public static void main(String[] args) throws IOException {
        if(args.length==0||args[0].equals("-h")){
            System.out.println("usage:\nargs0:hive hostname(like hdfs://bdpe**:9000);\nargs1:scale(1 means 1g)");
        }else{
            Configuration conf = new Configuration();
            conf.set("fs.default.name", args[0]);
            Path path = new Path("/user/hive/warehouse/extra-nested");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(path)){
                fs.delete(path,true);
            }
            OutputStream os = fs.create(path);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os));
            Random random = new Random();
            long m,n;
            if(args.length==1){
                m=1;
            }else{
                m = Integer.valueOf(args[1]).intValue();
            }
            byte[] bytebb = new byte[]{2};
            byte[] bytecc = new byte[]{3};
            String bb = new String(bytebb);
            String cc = new String(bytecc);
            n=(long)1500000*m;
            for (long i = 0; i < n; i++) {
                StringBuffer s = new StringBuffer();
                String ss[] = new String[27];
                for (int j=0;j<27;j++){
                    ss[j]=randomString(20);
                }
                s.append(ss[0]).append(bb).append(ss[1]).append(bb).append(ss[2]).append(bb).append(ss[3]).append(bb).append(ss[4]).append(cc).append(ss[5]).append(bb)
                        .append(ss[6]).append(bb).append(ss[7]).append(bb).append(ss[8]).append(bb).append(random.nextBoolean()).append(bb).append(ss[9]).append(bb)
                        .append(ss[10]).append(bb).append(ss[11]).append(bb).append(ss[12]).append(bb).append(random.nextLong()).append(bb).append(ss[13]).append(bb)
                        .append(ss[14]).append(bb).append(ss[15]).append(cc).append(ss[16]).append(bb).append(random.nextLong()).append(bb).append(ss[17]).append(bb).append(ss[18]).append(cc).append(ss[19]).append(bb)
                        .append(ss[20]).append(bb).append(ss[21]).append(cc).append(ss[22]).append(bb).append(ss[23]).append(bb).append(random.nextLong()).append(bb).append(random.nextLong()).append(bb)
                        .append(ss[24]).append(bb).append(ss[25]).append(bb).append(random.nextLong()).append(bb).append(ss[26]).append(bb).append(random.nextLong()).append("\n");
                bw.write(s.toString());
            }
            bw.flush();
            bw.close();
        }
    }
    public static String randomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int num = random.nextInt(62);
            buf.append(str.charAt(num));
        }
        return buf.toString();
    }
}
