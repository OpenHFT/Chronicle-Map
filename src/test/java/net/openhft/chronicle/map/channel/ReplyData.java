package net.openhft.chronicle.map.channel;

interface ReplyData {
    void status(boolean ok);

    void reply(DummyData t);

    void goodbye();
}
