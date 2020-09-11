/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.crypto;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.JavaUtils;

public class AuthIntegrationSuite {

  private AuthTestCtx ctx;

  @After
  public void cleanUp() throws Exception {
    if (ctx != null) {
      ctx.close();
    }
    ctx = null;
  }

  @Test
  public void testNewAuth() throws Exception {
    ctx = new AuthTestCtx();
    ctx.createServer("secret");
    ctx.createClient("secret");

    ByteBuffer reply = ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000);
    assertEquals("Pong", JavaUtils.bytesToString(reply));
    assertNull(ctx.authRpcHandler.saslHandler);
  }

  @Test
  public void testAuthFailure() throws Exception {
    ctx = new AuthTestCtx();
    ctx.createServer("server");

    try {
      ctx.createClient("client");
      fail("Should have failed to create client.");
    } catch (Exception e) {
      assertFalse(ctx.authRpcHandler.isAuthenticated());
      assertFalse(ctx.serverChannel.isActive());
    }
  }

  @Test
  public void testSaslServerFallback() throws Exception {
    ctx = new AuthTestCtx();
    ctx.createServer("secret", true);
    ctx.createClient("secret", false);

    ByteBuffer reply = ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000);
    assertEquals("Pong", JavaUtils.bytesToString(reply));
    assertNotNull(ctx.authRpcHandler.saslHandler);
    assertTrue(ctx.authRpcHandler.isAuthenticated());
  }

  @Test
  public void testSaslClientFallback() throws Exception {
    ctx = new AuthTestCtx();
    ctx.createServer("secret", false);
    ctx.createClient("secret", true);

    ByteBuffer reply = ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000);
    assertEquals("Pong", JavaUtils.bytesToString(reply));
  }

  @Test
  public void testAuthReplay() throws Exception {
    // This test covers the case where an attacker replays a challenge message sniffed from the
    // network, but doesn't know the actual secret. The server should close the connection as
    // soon as a message is sent after authentication is performed. This is emulated by removing
    // the client encryption handler after authentication.
    ctx = new AuthTestCtx();
    ctx.createServer("secret");
    ctx.createClient("secret");

    assertNotNull(ctx.client.getChannel().pipeline()
      .remove(TransportCipher.ENCRYPTION_HANDLER_NAME));

    try {
      ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000);
      fail("Should have failed unencrypted RPC.");
    } catch (Exception e) {
      assertTrue(ctx.authRpcHandler.isAuthenticated());
    }
  }

  @Test
  public void testLargeMessageEncryption() throws Exception {
    // Use a big length to create a message that cannot be put into the encryption buffer completely
    final int testErrorMessageLength = TransportCipher.STREAM_BUFFER_SIZE;
    ctx = new AuthTestCtx(new RpcHandler() {
      @Override
      public void receive(
          TransportClient client,
          ByteBuffer message,
          RpcResponseCallback callback) {
        char[] longMessage = new char[testErrorMessageLength];
        Arrays.fill(longMessage, 'D');
        callback.onFailure(new RuntimeException(new String(longMessage)));
      }

      @Override
      public StreamManager getStreamManager() {
        return null;
      }
    });
    ctx.createServer("secret");
    ctx.createClient("secret");

    try {
      ctx.client.sendRpcSync(JavaUtils.stringToBytes("Ping"), 5000);
      fail("Should have failed unencrypted RPC.");
    } catch (Exception e) {
      assertTrue(ctx.authRpcHandler.isAuthenticated());
      assertTrue(e.getMessage() + " is not an expected error", e.getMessage().contains("DDDDD"));
      // Verify we receive the complete error message
      int messageStart = e.getMessage().indexOf("DDDDD");
      int messageEnd = e.getMessage().lastIndexOf("DDDDD") + 5;
      assertEquals(testErrorMessageLength, messageEnd - messageStart);
    }
  }

}
