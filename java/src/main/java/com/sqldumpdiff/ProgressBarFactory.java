package com.sqldumpdiff;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

/**
 * Factory for creating progress bars that respect debug mode.
 * In debug mode, progress bars are disabled.
 */
public class ProgressBarFactory {
    private final boolean debug;

    public ProgressBarFactory(boolean debug) {
        this.debug = debug;
    }

    public ProgressBar create(String taskName, long maxValue) {
        if (debug) {
            // Return a no-op progress bar
            return new ProgressBarBuilder()
                    .setTaskName(taskName)
                    .setInitialMax(maxValue)
                    .setStyle(ProgressBarStyle.ASCII)
                    .setConsumer(new NoOpProgressBarConsumer())
                    .build();
        } else {
            return new ProgressBarBuilder()
                    .setTaskName(taskName)
                    .setInitialMax(maxValue)
                    .setStyle(ProgressBarStyle.ASCII)
                    .build();
        }
    }

    public ProgressBar create(String taskName, long maxValue, String unit) {
        if (debug) {
            // Return a no-op progress bar
            return new ProgressBarBuilder()
                    .setTaskName(taskName)
                    .setInitialMax(maxValue)
                    .setUnit(unit, 1)
                    .setStyle(ProgressBarStyle.ASCII)
                    .setConsumer(new NoOpProgressBarConsumer())
                    .build();
        } else {
            return new ProgressBarBuilder()
                    .setTaskName(taskName)
                    .setInitialMax(maxValue)
                    .setUnit(unit, 1)
                    .setStyle(ProgressBarStyle.ASCII)
                    .build();
        }
    }
}
