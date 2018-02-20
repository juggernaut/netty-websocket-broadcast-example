package co.leanjava.pubsub;

import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ameya
 */
public class WebSocketMessageHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    public WebSocketMessageHandler() {
        super(false);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketMessageHandler.class);

    private static final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
        if (frame instanceof TextWebSocketFrame) {
            final String text = ((TextWebSocketFrame) frame).text();
            LOGGER.info("Received text frame {}", text);

            PromiseCombiner promiseCombiner = new PromiseCombiner();
            allChannels.stream()
                    .filter(c -> c != ctx.channel())
                    //.forEach(c -> c.writeAndFlush(frame.copy()));
                    .forEach(c -> {
                        frame.retain();
                        promiseCombiner.add(c.writeAndFlush(frame.duplicate()).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (!future.isSuccess()) {
                                    LOGGER.info("Failed to write to channel: {}", future.cause());
                                }
                            }
                        }));
                    });

            Promise aggPromise = ctx.newPromise();
            promiseCombiner.finish(aggPromise);

            aggPromise.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (frame.release()) {
                        LOGGER.debug("WebSocket frame successfully deallocated");
                    } else {
                        LOGGER.warn("WebSocket frame leaked!");
                    }
                }
            });

        } else {
            throw new UnsupportedOperationException("Invalid websocket frame received");
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.info("Adding new channel {} to list of channels", ctx.channel().remoteAddress());
        allChannels.add(ctx.channel());
    }
}
