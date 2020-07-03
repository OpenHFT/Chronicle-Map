package net.openhft.chronicle.map.fromdocs.acid.revelations;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.fromdocs.BondVOInterface;


import java.io.IOException;
import java.util.concurrent.locks.StampedLock;

import static net.openhft.chronicle.values.Values.newNativeReference;

/**
 * ben.cotton@rutgers.edu
 * <p>
 * A totally hacked impl, awaiting OpenHFT's official ChronicleStampedLock API
 */
public class ChronicleStampedLock extends StampedLock {

}
