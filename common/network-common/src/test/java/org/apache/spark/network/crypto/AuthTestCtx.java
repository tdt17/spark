package org.apache.spark.network.crypto;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.Channel;
import org.apache.spark.network.TestUtils;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AuthTestCtx {

  private final String appId = "testAppId";
  private final TransportConf conf;
  private final TransportContext ctx;

  TransportClient client;
  TransportServer server;
  volatile Channel serverChannel;
  volatile AuthRpcHandler authRpcHandler;

  AuthTestCtx() throws Exception {
    this(new RpcHandler() {
      @Override
      public void receive(
          TransportClient client,
          ByteBuffer message,
          RpcResponseCallback callback) {
        assertEquals("Ping", JavaUtils.bytesToString(message));
        callback.onSuccess(JavaUtils.stringToBytes("Pong"));
      }

      @Override
      public StreamManager getStreamManager() {
        return null;
      }
    });
  }

  AuthTestCtx(RpcHandler rpcHandler) throws Exception {
    Map<String, String> testConf = ImmutableMap.of("spark.network.crypto.enabled", "true");
    this.conf = new TransportConf("rpc", new MapConfigProvider(testConf));
    this.ctx = new TransportContext(conf, rpcHandler);
  }

  void createServer(String secret) throws Exception {
    createServer(secret, true);
  }

  void createServer(String secret, boolean enableAes) throws Exception {
    TransportServerBootstrap introspector = (channel, rpcHandler) -> {
      this.serverChannel = channel;
      if (rpcHandler instanceof AuthRpcHandler) {
        this.authRpcHandler = (AuthRpcHandler) rpcHandler;
      }
      return rpcHandler;
    };
    SecretKeyHolder keyHolder = createKeyHolder(secret);
    TransportServerBootstrap auth = enableAes ? new AuthServerBootstrap(conf, keyHolder)
      : new SaslServerBootstrap(conf, keyHolder);
    this.server = ctx.createServer(Arrays.asList(auth, introspector));
  }

  void createClient(String secret) throws Exception {
    createClient(secret, true);
  }

  void createClient(String secret, boolean enableAes) throws Exception {
    TransportConf clientConf = enableAes ? conf
      : new TransportConf("rpc", MapConfigProvider.EMPTY);
    List<TransportClientBootstrap> bootstraps = Arrays.asList(
      new AuthClientBootstrap(clientConf, appId, createKeyHolder(secret)));
    this.client = ctx.createClientFactory(bootstraps)
      .createClient(TestUtils.getLocalHost(), server.getPort());
  }

  void close() {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.close();
    }
    if (ctx != null) {
      ctx.close();
    }
  }

  private SecretKeyHolder createKeyHolder(String secret) {
    SecretKeyHolder keyHolder = mock(SecretKeyHolder.class);
    when(keyHolder.getSaslUser(anyString())).thenReturn(appId);
    when(keyHolder.getSecretKey(anyString())).thenReturn(secret);
    return keyHolder;
  }

}
