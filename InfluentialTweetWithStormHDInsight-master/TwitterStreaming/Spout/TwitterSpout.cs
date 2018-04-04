using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;
using Tweetinvi;
using System.Configuration;
using Tweetinvi.Models;
using Tweetinvi.Streaming;
using System.Diagnostics;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TwitterStreaming.Data;
using System.Collections.Concurrent;

namespace TwitterStreaming.Spout
{
public class TwitterSpout : ISCPSpout
{
    private Context context;
    Thread listenerThread;

    long seqId = 0;
    Dictionary<long, ITweet> cache = new Dictionary<long, ITweet>(10000);
    private bool enableAck = false;

    public static List<Type> OutputSchema = new List<Type>() { typeof(SerializableTweet) };
    public static List<string> OutputSchemaName = new List<string>() { "SerializableTweet" };

    public TwitterSpout(Context ctx)
    {
        this.context = ctx;

        Dictionary<string, List<Type>> outSchema = new Dictionary<string, List<Type>>();
        outSchema.Add("default", OutputSchema);
        this.context.DeclareComponentSchema(new ComponentStreamSchema(null, outSchema));

        // Get pluginConf info and enable ACK in Non-Tx topology
        if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
        {
            enableAck = (bool)(Context.Config.pluginConf
                    [Constants.NONTRANSACTIONAL_ENABLE_ACK]);
        }
        Context.Logger.Info("enableAck: {0}", enableAck);

        listenerThread = new Thread(new ThreadStart(StartStream));
        listenerThread.Start();
    }

    public static TwitterSpout Get(Context ctx, Dictionary<string, Object> parms)
    {
        return new TwitterSpout(ctx);
    }

    public void NextTuple(Dictionary<string, Object> parms)
    {
        if (queue.Count > 0)
        {
            var tweet = queue.Dequeue();
            cache.Add(seqId++, tweet);

            this.context.Emit(Constants.DEFAULT_STREAM_ID,
                new Values(new SerializableTweet(tweet)), seqId);

            Context.Logger.Info("Emit: {0}, seqId: {1}", tweet.FullText, seqId);
        }
    }

        public void Ack(long seqId, Dictionary<string, object> parms)
        {
            Context.Logger.Info("Ack, seqId: {0}", seqId);
            if (!cache.Remove(seqId))
                Context.Logger.Warn("Ack(), remove cached tuple for seqId {0} fail!", seqId);
        }

        public void Fail(long seqId, Dictionary<string, object> parms)
        {
            Context.Logger.Info("Fail, seqId: {0}", seqId);
            if (cache.ContainsKey(seqId))
            {
                ITweet tweet = cache[seqId];
                Context.Logger.Info("Re-Emit: {0}, seqId: {1}", tweet.FullText, seqId);
                this.context.Emit(Constants.DEFAULT_STREAM_ID, new Values(tweet.FullText), seqId);
            }
            else
            {
                Context.Logger.Warn("Fail(), can't find cached tuple for seqId {0}!", seqId);
            }
        }

        Queue<ITweet> queue = new Queue<ITweet>();
        private void StartStream()
        {
            Auth.SetUserCredentials(
                ConfigurationManager.AppSettings["ConsumerKey"],
                ConfigurationManager.AppSettings["ConsumerSecret"],
                ConfigurationManager.AppSettings["AccessToken"],
                ConfigurationManager.AppSettings["AccessTokenSecret"]);

            var stream = Tweetinvi.Stream.CreateSampleStream();
            stream.AddTweetLanguageFilter(LanguageFilter.English);
            stream.TweetReceived += (s, e) =>
            {
                if (e.Tweet.IsRetweet)
                    queue.Enqueue(e.Tweet.RetweetedTweet);
            };
            stream.StartStream();
        }
    }
}