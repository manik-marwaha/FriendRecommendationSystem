package util;

import org.apache.hadoop.util.Tool;

// Import Files  
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Apache Hadoop Import Files
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendRecommend {

	static public class countFriends implements Writable {

		public Long id;                 // user Id
		public Long friends_mutual;    // mutual friends

		// constructor
		public countFriends(Long id, Long friends_mutual) {
			this.id = id;
			this.friends_mutual = friends_mutual;
		}

		public countFriends() {
			this(-1L, -1L);
		}

		// function to read input data
		public void readFields(DataInput in) throws IOException {
			id = in.readLong();                    
			friends_mutual = in.readLong();       
		}
		
		// function to write output data
		public void writeOutput(DataOutput out) throws IOException {
			out.writeLong(id);
			out.writeLong(friends_mutual);
		}

		// function to convert to string
		public String toString() {
			return " forUser: "+ Long.toString(id) + " Mutual Friends: " + Long.toString(friends_mutual);
		}		
	
	}

	// Mapper Class
	// create class to map for every user
	public static class FriendMapper extends Mapper <LongWritable, Text, LongWritable, countFriends> {
		
		public void doMap(LongWritable key, Text txt, Context ctext) throws InterruptedException,IOException {

			// list of friends
			List <Long> friends = new ArrayList <Long> (); 
			
			// storing each entry of the text file (where tab separates user from friend) 
			// in a string array 'text_array' by splitting text on the basis of tab
			String text_array [] = txt.toString().split("\t");  
			
			// variable 'user' stores the user id and parses string to long 
			Long user = Long.parseLong(text_array[0]); 	
			
			if (text_array.length == 2) {
				
				// creating an instance of StrongTokeniser, all the friends of the user are seperated by comma in text_array[1]
				StringTokenizer token = new StringTokenizer(text_array[1], ",");
				
				// all the friends of the user are added to the list 'friends'
				while (token.hasMoreTokens()) {
					Long forUser = Long.parseLong(token.nextToken());
					friends.add(forUser);
					ctext.write(new LongWritable(user), new countFriends(forUser, -1L));  // <key - user, value - friend of user, -1 > (as pair are already friends)
				}
				
				// loop to generate all possible pair of friends from friends of user 
				for (int i = 0; i < friends.size(); i++) {
					for (int j = i + 1; j < friends.size(); j++) {

						ctext.write(new LongWritable(friends.get(i)), new countFriends((friends.get(j)), user)); // ex: <key - user1,val - user2,User> (here, User is mutual friend of both user1 and user2)
						ctext.write(new LongWritable(friends.get(j)), new countFriends((friends.get(i)), user)); 
					}
				}
			}
		}
	}
	
	// Reducer Class
	// Key -> Recommended Friend, Value -> List of Mutual Friend
	public static class FriendReducer extends Reducer <LongWritable, countFriends, LongWritable, Text> {
		
		public void reduce(LongWritable key, Iterable<countFriends> val, Context ctext) throws IOException, InterruptedException {
			
			// creating an instance of Hash function where value of hash function are mutual friends
			final java.util.Map <Long, List<Long>> friends_mutual = new HashMap <Long, List<Long>>();
			
			// finding the mutual friends 
			for (countFriends value : val) {  

				final Boolean isFriend = (value.friends_mutual == -1);   //friends_mutual equals to -1 when user and recomm friend are already friends 
				final Long forUser = value.id;                           //recommendation to user
				final Long mutual_Friend = value.friends_mutual;

				if (friends_mutual.containsKey(forUser)) {

					// if already friends
					if (isFriend) {                          
						friends_mutual.put(forUser, null);   //don't recommend the friend who is already friends with User

					} else if (friends_mutual.get(forUser) != null) {
						friends_mutual.get(forUser).add(mutual_Friend);  
					}
				} else {

					// if not friends with User
					if (!isFriend) {
						friends_mutual.put(forUser, new ArrayList<Long>() {
							{
								add(mutual_Friend);    
							}
						});
					} else {
						friends_mutual.put(forUser, null);
					}
				}
			}

			// Sorting all the Mutual friends using Tree Map
			java.util.SortedMap<Long, List<Long>> sort_friend = new TreeMap<Long, List<Long>>(new Comparator<Long>() {

				public int compare(Long key_1, Long key_2) {

					int k1_size = friends_mutual.get(key_1).size();
					int k2_size = friends_mutual.get(key_2).size();
					
					// the recommended friends sorted in decreasing order and for same mutual friends ascending order of id
					if (k1_size > k2_size) {
						return -1;
					} else if ((k1_size==k2_size) && key_1 < key_2) {
						return -1;
					} else {
						return 1;
					}
				}
			});

			for (java.util.Map.Entry<Long, List<Long>> list_entry : friends_mutual.entrySet()) {
				if (list_entry.getValue() != null) {
					sort_friend.put(list_entry.getKey(), list_entry.getValue());
				}
			}

			int k = 0;         //no. of recommended friends
			String output_txt = "";  

			// write output
			for (java.util.Map.Entry<Long, List<Long>> list_entry : sort_friend.entrySet()) {

				if (k == 0) {
					output_txt = list_entry.getKey().toString();
				} else if (k < 10){
					output_txt += "," + list_entry.getKey().toString();
				}
				++k;
			}
			ctext.write(key, new Text(output_txt));
		}
	}
	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();					      //creating an instance for configuration

		Job job = new Job(conf, "Friend Recommendation System");	     // new job for the configuration
		job.setJarByClass(FriendRecommend.class);				     
		job.setOutputKeyClass(LongWritable.class);                      //set output keyclass
		job.setOutputValueClass(countFriends.class);                   //set output value class
		job.setMapperClass(FriendMapper.class);						  // set Mapper class
		job.setReducerClass(FriendReducer.class);					 // set reducer class
		job.setInputFormatClass(TextInputFormat.class);			
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);
		
		// adding input and output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
	
}
	