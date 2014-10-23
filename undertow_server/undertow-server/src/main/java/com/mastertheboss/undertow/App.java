package com.mastertheboss.undertow;

import io.undertow.Undertow;
import io.undertow.server.*;
import io.undertow.util.Headers;
import io.undertow.*;
import io.undertow.server.handlers.*;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.math.BigInteger;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(final String[] args) {

        HttpHandler helloworld = new HttpHandler() {
                    public void handleRequest(final HttpServerExchange exchange)
                            throws Exception {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                                "text/plain");
                        exchange.getResponseSender().send("Mom. I'm alive.");
                        }
                    };

        final BigInteger publicKey= new BigInteger("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153");
        final SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
        HttpHandler q1Handler = new HttpHandler(){
            public void handleRequest(final HttpServerExchange exchange)
                    throws Exception {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
                        "text/plain");
                String key_str = exchange.getQueryParameters().get("key").getFirst();
                BigInteger key = new BigInteger(key_str);
                BigInteger number = key.divide(publicKey);
                String timeStr = timeFormat.format(Calendar.getInstance().getTime());

                String output = String.format(
                    "%s\nAmazombies,jiajunwa,chiz2,sanchuah\n%s",
                    number.toString(),
                    timeStr
                );
                exchange.getResponseSender().send(
                    output
                );
            }
        };

        PathHandler pathhandler = Handlers.path();
        pathhandler.addPrefixPath("/q1", q1Handler);

        pathhandler.addPrefixPath("/", helloworld);


        Undertow server = Undertow.builder().addHttpListener(8888, "localhost")
                .setHandler(pathhandler).build();
        server.start();
    }
}
