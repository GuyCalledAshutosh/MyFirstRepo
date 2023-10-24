import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input format is ["a", 0, 0, 63]
        String[] csv = value.toString().split(",");
        String matrix = csv[0].trim();
        int row = Integer.parseInt(csv[1].trim());
        int col = Integer.parseInt(csv[2].trim();

        if (matrix.equals("a")) {
            for (int i = 0; i < lMax; i++) {
                String akey = row + "," + i;
                context.write(new Text(akey), value);
            }
        } else if (matrix.equals("b")) {
            for (int i = 0; i < iMax; i++) {
                String bkey = i + "," + col;
                context.write(new Text(bkey), value);
            }
        }
    }
}

mapper

reducer
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Determine the size of the matrices based on the number of values
        int matrixSize = determineMatrixSize(values);
        
        int[] a = new int[matrixSize];
        int[] b = new int[matrixSize];

        for (Text value : values) {
            String cell[] = value.toString().split(",");
            if (cell[0].contains("a")) {
                int col = Integer.parseInt(cell[2].trim());
                a[col] = Integer.parseInt(cell[3].trim());
            } else if (cell[0].contains("b")) {
                int row = Integer.parseInt(cell[1].trim());
                b[row] = Integer.parseInt(cell[3].trim());
            }
        }

        int total = 0;
        for (int i = 0; i < matrixSize; i++) {
            total += a[i] * b[i];
        }
        context.write(key, new IntWritable(total));
    }

    private int determineMatrixSize(Iterable<Text> values) {
        int maxSize = 0;

        for (Text value : values) {
            String cell[] = value.toString().split(",");
            int matrixIndex = cell[0].contains("a") ? 2 : 1;
            int index = Integer.parseInt(cell[matrixIndex].trim());
            if (index > maxSize) {
                maxSize = index;
            }
        }

        // Add 1 to account for 0-based indices
        return maxSize + 1;
    }
}
