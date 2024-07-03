using CK.Core;
using CK.MQTT;
using CK.MQTT.Client;
using CK.MQTT.LowLevelClient.PublishPackets;
using Microsoft.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace CK.Monitoring.Handlers
{
    public sealed class MQTT : BaseLogSender<MQTTConfiguration>
    {
        class MQTTSender : ISender
        {
            readonly IMQTT3Client _client;
            bool _connected = true;
            public bool IsActuallyConnected => _client.IsConnected;
            public QualityOfService QoS { get; set; }


            public MQTTSender( IMQTT3Client client, QualityOfService qos )
            {
                _client = client;
                QoS = qos;
            }

            static readonly Encoding _encoding = new UTF8Encoding();
            public async ValueTask<bool> TrySendAsync( IActivityMonitor monitor, IFullLogEntry logEvent )
            {
                var mem = (RecyclableMemoryStream)Util.RecyclableStreamManager.GetStream( "CK.Monitoring.MQTT" );
                CKBinaryWriter bw = new( mem, _encoding, false );
                logEvent.WriteLogEntry( bw );
                mem.Position = 0;
                await _client!.PublishAsync(
                    new StreamApplicationMessage(
                        mem,
                        false,
                        $"ck-log/{logEvent.GrandOutputId}",
                        QoS,
                        false
                    )
                );
                return _connected;
            }

            public void SetDisconnected() => _connected = false;

            public async ValueTask DisposeAsync()
            {
                await _client.DisconnectAsync( true );
                await _client.DisposeAsync();
            }
        }

        IMQTT3Client _client = null!;
        public MQTT( MQTTConfiguration config ) : base( config )
        {
        }

        public override async ValueTask<bool> ActivateAsync( IActivityMonitor monitor )
        {
            _client = MQTTClient.Factory.WithConfig(
                 new MQTT3ClientConfiguration()
                 {
                     KeepAliveSeconds = 0,
                     ClientId = "ck-log-" + CoreApplicationIdentity.InstanceId,
                     ConnectionString = Configuration.ConnectionString
                 } )
                .WithAutoReconnect()
                .Build();
            var res = await _client.ConnectAsync( null );
            if( res.Status != ConnectStatus.Successful )
            {
                monitor.Error( $"Unrecoverable error while connecting :{res}" );
            }
            return await base.ActivateAsync( monitor );
        }

        public override ValueTask<bool> ApplyConfigurationAsync( IActivityMonitor m, IHandlerConfiguration c )
        {
            if( c is not MQTTConfiguration cfg ) return new ValueTask<bool>( false );
            UpdateConfiguration( m, cfg );
            var sender = _sender;
            if( sender != null ) sender.QoS = cfg.QoS;
            return new( true );
        }
        MQTTSender? _sender;
        protected override Task<ISender?> CreateSenderAsync( IActivityMonitor monitor )
        {
            if( !_client.IsConnected ) return Task.FromResult<ISender?>( null );
            var newSender = new MQTTSender( _client, Configuration.QoS );
            _sender = newSender;
            return Task.FromResult<ISender?>( newSender );
        }

        public override async ValueTask DeactivateAsync( IActivityMonitor m )
        {
            await base.DeactivateAsync( m );
            await _client!.DisconnectAsync( true );
        }
    }
}
