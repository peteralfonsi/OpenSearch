package org.opensearch.lucene.cache;

import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.file.Files;
import java.util.OptionalLong;

/**
 * Directory impl which always uses IO for testing purposes.
 */
public class ForcedDirectIODirectory extends DirectIODirectory {

    int blockSize;
    private long minBytesDirect = 10 * 1024 * 1024;

    public ForcedDirectIODirectory(FSDirectory delegate, int mergeBufferSize, long minBytesDirect) throws IOException {
        super(delegate, mergeBufferSize, minBytesDirect);
        this.blockSize = Math.toIntExact(Files.getFileStore(delegate.getDirectory()).getBlockSize());
        System.out.println("====Block size returned from file system is ===" + blockSize);
    }

    public ForcedDirectIODirectory(FSDirectory delegate) throws IOException {
        super(delegate);
        this.blockSize = Math.toIntExact(Files.getFileStore(delegate.getDirectory()).getBlockSize());
        System.out.println("====Block size returned from file system is ===" + blockSize);
    }

    protected boolean useDirectIO(String name, IOContext context, OptionalLong fileLength) {
        // If MERGE, fall back to logic used in DirectIODirectory. Otherwise return true (DirectIODirectory would have returned false)
        if (context.context().equals(IOContext.Context.MERGE)) {
            return context.mergeInfo().estimatedMergeBytes() >= minBytesDirect
                && fileLength.orElse(minBytesDirect) >= minBytesDirect; 
        }
        return true;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        return in.createOutput(name, context);
    }
}
