using System.ComponentModel.DataAnnotations;

namespace Messagefy.Kafka.Configuration;

public class KafkaConsumerOptions
{
    [Required]
    public string BootstrapServers { get; set; }
    
    [Required]
    public string GroupId { get; set; }
    
    [Required]
    public string Topic { get; set; }

    [Required]
    public string SchemaRegistryUrl { get; set; }

    public int CommitOffsetPeriod { get; set; } = 1;

    public bool? EnableAutoCommit { get; set; }
}