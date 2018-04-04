using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tweetinvi.Models;

namespace TwitterStreaming.Data
{
[Serializable]
public class SerializableTweet
{
    public string Text { get; set; }
    public long Id { get; set; }
    public int RetweetCount { get; set; }
    public int FavoriteCount { get; set; }
    public decimal Score
    {
        get
        {
            return (RetweetCount + FavoriteCount);
        }
    }

    public SerializableTweet()
    {
        // For searialization and deserialization
    }

    public SerializableTweet(ITweet tweet)
    {
        this.Text = tweet.FullText;
        this.Id = tweet.Id;
        this.RetweetCount = tweet.RetweetCount;
        this.FavoriteCount = tweet.FavoriteCount;
    }

    public override string ToString()
    {
        return $"{Id.ToString()}:{Text}:Retweet-{RetweetCount}:Likes-{FavoriteCount}:Score-{Score}";
    }
}
}
