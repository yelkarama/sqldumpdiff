package com.sqldumpdiff;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.Blake3;

/**
 * BLAKE3 hashing with native fast path (Panama) and pure-Java fallback.
 *
 * Native path requires a shared library named "blake3" on the library path.
 * If unavailable, it falls back to commons-codec Blake3.
 */
public final class Blake3Hasher {
    // Size of blake3_hasher struct in C (bytes). This is stable in upstream.
    private static final int HASHER_SIZE = 1912;

    private static final boolean NATIVE_AVAILABLE;
    private static final String NATIVE_ERROR;
    private static final MethodHandle MH_INIT;
    private static final MethodHandle MH_UPDATE;
    private static final MethodHandle MH_FINAL;

    static {
        MethodHandle init = null;
        MethodHandle update = null;
        MethodHandle fin = null;
        boolean nativeAvailable;
        String nativeError = null;
        try {
            tryLoadNative();
            Linker linker = Linker.nativeLinker();
            SymbolLookup lookup = SymbolLookup.loaderLookup();
            init = linker.downcallHandle(
                    lookup.find("blake3_hasher_init").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
            );
            update = linker.downcallHandle(
                    lookup.find("blake3_hasher_update").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
            );
            fin = linker.downcallHandle(
                    lookup.find("blake3_hasher_finalize").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
            );
            nativeAvailable = true;
        } catch (Throwable t) {
            nativeAvailable = false;
            nativeError = t.toString();
        }
        NATIVE_AVAILABLE = nativeAvailable;
        MH_INIT = init;
        MH_UPDATE = update;
        MH_FINAL = fin;
        NATIVE_ERROR = nativeError;
    }

    private Blake3Hasher() {}

    public static byte[] hashBytes(byte[] input) {
        if (NATIVE_AVAILABLE) {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment hasher = arena.allocate(HASHER_SIZE);
                MH_INIT.invoke(hasher);
                MemorySegment data = arena.allocate(input.length);
                data.copyFrom(MemorySegment.ofArray(input));
                MH_UPDATE.invoke(hasher, data, (long) input.length);
                MemorySegment out = arena.allocate(32);
                MH_FINAL.invoke(hasher, out, 32L);
                byte[] result = out.toArray(ValueLayout.JAVA_BYTE);
                return result;
            } catch (Throwable t) {
                // fall back below
            }
        }
        Blake3 hasher = Blake3.initHash();
        hasher.update(input);
        return hasher.doFinalize(32);
    }

    public static String hashHex(String input) {
        byte[] hash = hashBytes(input.getBytes(StandardCharsets.UTF_8));
        return Hex.encodeHexString(hash);
    }

    public static boolean isNativeAvailable() {
        return NATIVE_AVAILABLE;
    }

    public static boolean debugNativeStatus() {
        tryLoadNative();
        return NATIVE_AVAILABLE;
    }

    public static String nativeError() {
        return NATIVE_ERROR;
    }

    private static void tryLoadNative() {
        // First try normal library lookup.
        try {
            System.loadLibrary("blake3");
            return;
        } catch (UnsatisfiedLinkError ignored) {
        }

        // Then try explicit paths (user override + common Homebrew locations).
        List<String> candidates = new ArrayList<>();
        String override = System.getProperty("blake3.path");
        if (override != null && !override.isBlank()) {
            candidates.add(override);
        }
        candidates.add("/opt/homebrew/lib/libblake3.dylib");
        candidates.add("/opt/homebrew/Cellar/blake3/1.8.3/lib/libblake3.dylib");
        for (String p : candidates) {
            try {
                boolean exists = Files.exists(Path.of(p));
                System.err.println("[INFO] BLAKE3 candidate: " + p + " exists=" + exists);
                if (exists) {
                    System.err.println("[INFO] Trying to load native BLAKE3: " + p);
                    System.load(p);
                    return;
                }
            } catch (UnsatisfiedLinkError e) {
                System.err.println("[WARN] Failed to load native BLAKE3: " + p + " (" + e.getMessage() + ")");
            }
        }
    }

    // Logging handled at startup via SqlDumpDiff.
}
