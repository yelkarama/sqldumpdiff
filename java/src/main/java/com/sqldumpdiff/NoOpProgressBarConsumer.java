package com.sqldumpdiff;

import me.tongfei.progressbar.ProgressBarConsumer;

/**
 * A no-op ProgressBarConsumer that does nothing.
 * Used in debug mode to disable progress bar output.
 */
public class NoOpProgressBarConsumer implements ProgressBarConsumer {
    @Override
    public int getMaxRenderedLength() {
        return 0;
    }

    @Override
    public void accept(String str) {
        // Do nothing
    }

    @Override
    public void close() {
        // Do nothing
    }
}
