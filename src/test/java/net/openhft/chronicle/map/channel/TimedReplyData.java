package net.openhft.chronicle.map.channel;

import net.openhft.chronicle.wire.converter.NanoTime;

interface TimedReplyData {
    ReplyData on(@NanoTime long timeNS);
}
