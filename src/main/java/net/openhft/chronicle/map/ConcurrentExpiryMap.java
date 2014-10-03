package net.openhft.chronicle.map;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * wraps a concurrent map and queue so that we can easily expire old entries
 *
 * @author Rob Austin.
 */

