using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;
using TwitterStreaming.Bolt;
using TwitterStreaming.Spout;

namespace TwitterStreaming
{
[Active(true)]
class Program : TopologyDescriptor
{
    public ITopologyBuilder GetTopologyBuilder()
    {
        TopologyBuilder topologyBuilder = new TopologyBuilder("TwitterStreaming" + DateTime.Now.ToString("yyyyMMddHHmmss"));
        topologyBuilder.SetSpout(
            "TwitterSpout",
            TwitterSpout.Get,
            new Dictionary<string, List<string>>()
            {
                {Constants.DEFAULT_STREAM_ID, TwitterSpout.OutputSchemaName}
            },
            1, true);

        var boltConfig = new StormConfig();
        boltConfig.Set("topology.tick.tuple.freq.secs", "20");
        topologyBuilder.SetBolt(
            "TopNTweetBolt",
            TopNTweetBolt.Get,
            new Dictionary<string, List<string>>(){
                {"TOPNTWEETS_STREAM", TopNTweetBolt.OutputSchemaName}
            }, 1, true)
            .shuffleGrouping("TwitterSpout")
            .addConfigurations(boltConfig);

        topologyBuilder.SetBolt(
            "AzureSqlBolt",
            AzureSqlBolt.Get,
            new Dictionary<string, List<string>>(),
            1).shuffleGrouping("TopNTweetBolt", "TOPNTWEETS_STREAM");

        return topologyBuilder;
    }
}
}

