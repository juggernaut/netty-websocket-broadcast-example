package co.leanjava.pubsub;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ameya
 */
public class WebSocketMessageHandler extends SimpleChannelInboundHandler<WebSocketFrame> {


    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketMessageHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
        if (frame instanceof TextWebSocketFrame) {
            LOGGER.info("Received text frame {}", ((TextWebSocketFrame) frame).text());
        } else {
            throw new UnsupportedOperationException("Invalid websocket frame received");
        }
    }
}
