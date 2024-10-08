using System.ComponentModel.DataAnnotations;

namespace Messagefy.Kafka.Configuration;

public class KafkaProducerOptions
{
    [Required]
    public string BootstrapServers { get; set; }
    
    [Required]
    public string Topic { get; set; }
    
    public bool? EnableIdempotence { get; set; }
    
    [Required]
    public string SchemaRegistryUrl { get; set; }
}