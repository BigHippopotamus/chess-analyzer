import java.io.*;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class ProcessUnits {
	public static Text serialize(Serializable o) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);

		oos.writeObject(o);
		oos.close();

		return new Text(Base64.getEncoder().encodeToString(baos.toByteArray()));
	}

	public static Object deserialize(Text t) throws IOException, ClassNotFoundException {
		String s = t.toString();
		byte[] data = Base64.getDecoder().decode(s);

		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));

		Object o = ois.readObject();
		ois.close();

		return o;
	}

	public static class OutputData implements Serializable {
		public boolean isWhite;
		public int won;
		public String opening;
		public String pgn;
		public boolean wasCheckmate;

		public OutputData(boolean isWhite, int won, String opening, String pgn, boolean wasCheckmate) {
			this.isWhite = isWhite;
			this.won = won;
			this.opening = opening;
			this.pgn = pgn;
			this.wasCheckmate = wasCheckmate;
		}
	}

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

			//Opening may contain commas, so it is separated from the rest of the data
			int opening_start = line.indexOf("\"");
			int opening_end = line.indexOf("\"", opening_start + 1) + 1;
			int pgn_start = opening_end + 1;
			int pgn_end = line.indexOf("\"", pgn_start + 1) + 1;


			String opening = line.substring(opening_start, opening_end);
			String pgn = line.substring(pgn_start, pgn_end);
			boolean wasCheckmate = (line.charAt(line.length() - 1) == '1') ? true : false;
			
			String safe_data = line.substring(0, line.indexOf("\""));
            String[] data = safe_data.split(",");

            Text outputKey = new Text();
            OutputData outputValue;
			//Text outputValue = new Text();

            /*
             * Inputs in format
             * game_type, white, black, winner, opening
             */

			int status = data[2].equals("White") ? 1 : (data[2].equals("Draw") ? 0 : -1);
            
			outputKey.set(data[0]);
			outputValue = new OutputData(true, status, opening, pgn, wasCheckmate);
			context.write(outputKey, serialize(outputValue)); 

			outputKey.set(data[1]);
			outputValue = new OutputData(false, -status, opening, pgn, wasCheckmate);
			context.write(outputKey, serialize(outputValue)); 
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
		static class StoreFraction {
			public static StoreFraction DEFAULT = new StoreFraction(0L, 0L);

			Long valid;
			Long total;

			private StoreFraction(Long valid, Long total) {
				this.valid = valid;
				this.total = total;
			}

			public StoreFraction(boolean isValid) {
				valid = isValid ? 1L : 0L;
				total = 1L;
			}

			public static StoreFraction sum(StoreFraction a, StoreFraction b) {
				return new StoreFraction(a.valid + b.valid, a.total + b.total);
			}
		}

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StoreFraction whiteWins = StoreFraction.DEFAULT;
			StoreFraction blackWins = StoreFraction.DEFAULT;
			HashMap<String, StoreFraction> openingWins = new HashMap<>();

			StringBuilder pgns = new StringBuilder(",PGN");

			Long gamesPlayed = 0L;
            for (Text value : values) {
				OutputData val = null;

				try {
					val = (OutputData) deserialize(value);
				} catch (ClassNotFoundException e) {}

				gamesPlayed++;

				boolean isWhite = val.isWhite;
				int won = val.won;
				String opening = val.opening;
				boolean wasCheckmate = val.wasCheckmate;

				if (isWhite)
					whiteWins = StoreFraction.sum(new StoreFraction(won == 1), whiteWins);
				else
					blackWins = StoreFraction.sum(new StoreFraction(won == 1), blackWins);

				openingWins.merge(opening, new StoreFraction(won == 1), StoreFraction::sum);

				if (won == -1 && wasCheckmate)
					pgns.append(String.format(",%s,%d", val.pgn, isWhite ? 0 : 1));
            }

			StringBuilder output = new StringBuilder("\"" + key.toString() + "\"");
			output.append(",WHITE");
			output.append(String.format(",%d,%d", whiteWins.valid, whiteWins.total));

			output.append(",BLACK");
			output.append(String.format(",%d,%d", blackWins.valid, blackWins.total));

			output.append(",OPEN");
			for (String opening : openingWins.keySet()) {
				StoreFraction fraction = openingWins.get(opening);
				output.append(String.format(",%s,%d,%d", opening, fraction.valid, fraction.total));
			}

			/*
			output.append(",PGN");
			for (String pgn : pgns)
				output.append(String.format(",%s", pgn));
			*/

			output.append(pgns.toString());

			Text outputValue = new Text();
			outputValue.set(output.toString());
			context.write(null, outputValue);
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
 
        Job job = new Job(conf, "Stats");
        job.setJarByClass(ProcessUnits.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);

        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }
}
