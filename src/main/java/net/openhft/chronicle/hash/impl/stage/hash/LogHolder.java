package net.openhft.chronicle.hash.impl.stage.hash;

import net.openhft.sg.Staged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Staged
public class LogHolder {
    public static final Logger LOG = LoggerFactory.getLogger(LogHolder.class);
}
