using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using Tweetinvi.Models;
using TwitterStreaming.Data;
using TwitterStreaming.Spout;

namespace TwitterStreaming.Bolt
{
public class TopNTweetBolt : ISCPBolt
{
    bool enableAck = false;
    private Context context;
    List<SerializableTweet> tweetCache = new List<SerializableTweet>();

    public static List<Type> OutputSchema = new List<Type>() { typeof(SerializableTweet) };
    public static List<string> OutputSchemaName = new List<string>() { "SerializableTweet" };

    public TopNTweetBolt(Context ctx, Dictionary<string, Object> parms)
    {
        this.context = ctx;

        // Input Schemas
        Dictionary<string, List<Type>> inSchema = new Dictionary<string, List<Type>>();
        // Default stream
        inSchema.Add(Constants.DEFAULT_STREAM_ID, TwitterSpout.OutputSchema);
        // Listen to the Tick tuple stream
        inSchema.Add(Constants.SYSTEM_TICK_STREAM_ID, new List<Type> { typeof(long) });

        // Output Schema to new stream named TopNTweets
        Dictionary<string, List<Type>> outSchema = new Dictionary<string, List<Type>>();
        outSchema.Add("TOPNTWEETS_STREAM", OutputSchema);

        this.context.DeclareComponentSchema(new ComponentStreamSchema(inSchema, outSchema));

        //If this task excepts acks we need to set enableAck as true in TopologyBuilder for it
        if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
        {
            enableAck = (bool)(Context.Config.pluginConf
                [Constants.NONTRANSACTIONAL_ENABLE_ACK]);
        }
        enableAck = true;
    }

    public static TopNTweetBolt Get(Context ctx, Dictionary<string, Object> parms)
    {
        return new TopNTweetBolt(ctx, parms);
    }

    int totalAck = 0;
    public void Execute(SCPTuple tuple)
    {
        var isTickTuple = tuple.GetSourceStreamId().Equals(Constants.SYSTEM_TICK_STREAM_ID);
        if (isTickTuple)
        {
            // Get top 10 higest score tweets from last time window
            Context.Logger.Debug($"Total tweets in window: {tweetCache.Count}");
            var topNTweets = tweetCache.OrderByDescending(o => o.Score).Take(Math.Min(10, tweetCache.Count)).ToList();

            // Emit it to TopNTweet Stream
            foreach (var tweet in topNTweets)
            {
                //this.context.Emit(StormConstants.TOPNTWEETS_STREAM, new Values(tweet.Text, tweet.Id, tweet.RetweetCount, tweet.FavoriteCount, tweet.UserFollowerCount, tweet.Score));
                this.context.Emit("TOPNTWEETS_STREAM", new Values(tweet));
            }

            // Remove all existing data and wait for new one
            tweetCache.Clear();
        }
        else
        {
            try
            {
                // Process tuple and then acknowledge it
                SerializableTweet tweet = tuple.GetValue(0) as SerializableTweet;

                if (!tweetCache.Any(o => o.Id.Equals(tweet.Id)))
                    tweetCache.Add(tweet);

                Context.Logger.Info(tweet.ToString());

                if (enableAck)
                {
                    this.context.Ack(tuple);
                    Context.Logger.Info("Total Ack: " + ++totalAck);
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error("An error occured while executing Tuple Id: {0}. Exception Details:\r\n{1}",
                    tuple.GetTupleId(), ex.ToString());

                //Fail the tuple if enableAck is set to true in TopologyBuilder so that the tuple is replayed.
                if (enableAck)
                {
                    this.context.Fail(tuple);
                }
            }
        }
    }
}
}