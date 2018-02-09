package co.leanjava.pubsub;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;

/**
 * @author ameya
 */
public class PubSubServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {

        final ChannelPipeline pipeline = channel.pipeline();

        pipeline.addLast(new HttpServerCodec())
                .addLast(new HttpObjectAggregator(65536))
                .addLast(new WebSocketServerCompressionHandler())
                .addLast(new WebSocketServerProtocolHandler("/", null, true))
                .addLast()

    }
}
