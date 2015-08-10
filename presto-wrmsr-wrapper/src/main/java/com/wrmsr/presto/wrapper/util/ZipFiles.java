package com.wrmsr.presto.wrapper.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ZipFiles
{
    // 4-byte number
    static private int swapEndian(int i)
    {
        return ((i & 0xff) << 24) + ((i & 0xff00) << 8) + ((i & 0xff0000) >>> 8)
                + ((i >>> 24) & 0xff);
    }

    // 2-byte number
    static private int swapEndian(short i)
    {
        return ((i & 0x00FF) << 8 | (i & 0xFF00) >>> 8);
    }

    /*
     * Zip file constants.
     */
    static final int kEOCDSignature = 0x06054b50;
    static final int kEOCDLen = 22;
    static final int kEOCDNumEntries = 8; // offset to #of entries in file
    static final int kEOCDSize = 12; // size of the central directory
    static final int kEOCDFileOffset = 16; // offset to central directory

    static final int kMaxCommentLen = 65535; // longest possible in ushort
    static final int kMaxEOCDSearch = (kMaxCommentLen + kEOCDLen);

    static final int kLFHSignature = 0x04034b50;
    static final int kLFHLen = 30; // excluding variable-len fields
    static final int kLFHNameLen = 26; // offset to filename length
    static final int kLFHExtraLen = 28; // offset to extra length

    static final int kCDESignature = 0x02014b50;
    static final int kCDELen = 46; // excluding variable-len fields
    static final int kCDEMethod = 10; // offset to compression method
    static final int kCDEModWhen = 12; // offset to modification timestamp
    static final int kCDECRC = 16; // offset to entry CRC
    static final int kCDECompLen = 20; // offset to compressed length
    static final int kCDEUncompLen = 24; // offset to uncompressed length
    static final int kCDENameLen = 28; // offset to filename length
    static final int kCDEExtraLen = 30; // offset to extra length
    static final int kCDECommentLen = 32; // offset to comment length
    static final int kCDELocalOffset = 42; // offset to local hdr

    static final int kCompressStored = 0; // no compression
    static final int kCompressDeflated = 8; // standard deflate

    /*
     * The values we return for ZipEntryRO use 0 as an invalid value, so we want
     * to adjust the hash table index by a fixed amount. Using a large value
     * helps insure that people don't mix & match arguments, e.g. to
     * findEntryByIndex().
     */
    static final int kZipEntryAdj = 10000;

    ByteBuffer mLEByteBuffer = ByteBuffer.allocate(4);

    static private int read4LE(RandomAccessFile f) throws IOException
    {
        return swapEndian(f.readInt());
    }

    public static void getPreamble(File file) throws Throwable
    {
        RandomAccessFile f = new RandomAccessFile(file, "r");
        long fileLength = f.length();

        if (fileLength < kEOCDLen) {
            throw new java.io.IOException();
        }

        long readAmount = kMaxEOCDSearch;
        if (readAmount > fileLength)
            readAmount = fileLength;

        f.seek(0);

        int header = read4LE(f);
        if (header == kEOCDSignature) {
            throw new RuntimeException("Found Zip archive, but it looks empty");
        } else if (header != kLFHSignature) {
            throw new RuntimeException("Not a Zip archive");
        }

        /*
         * Perform the traditional EOCD snipe hunt. We're searching for the End
         * of Central Directory magic number, which appears at the start of the
         * EOCD block. It's followed by 18 bytes of EOCD stuff and up to 64KB of
         * archive comment. We need to read the last part of the file into a
         * buffer, dig through it to find the magic number, parse some values
         * out, and use those to determine the extent of the CD. We start by
         * pulling in the last part of the file.
         */
        long searchStart = fileLength - readAmount;

        f.seek(searchStart);
        ByteBuffer bbuf = ByteBuffer.allocate((int) readAmount);
        byte[] buffer = bbuf.array();
        f.readFully(buffer);
        bbuf.order(ByteOrder.LITTLE_ENDIAN);

        /*
         * Scan backward for the EOCD magic. In an archive without a trailing
         * comment, we'll find it on the first try. (We may want to consider
         * doing an initial minimal read; if we don't find it, retry with a
         * second read as above.)
         */

        // EOCD == 0x50, 0x4b, 0x05, 0x06
        int eocdIdx;
        for (eocdIdx = buffer.length - kEOCDLen; eocdIdx >= 0; eocdIdx--) {
            if (buffer[eocdIdx] == 0x50 && bbuf.getInt(eocdIdx) == kEOCDSignature)
            {
                if (LOGV) {
                    Log.v(LOG_TAG, "+++ Found EOCD at index: " + eocdIdx);
                }
                break;
            }
        }

        if (eocdIdx < 0) {
            Log.d(LOG_TAG, "Zip: EOCD not found, " + zipFileName + " is not zip");
        }
    }
}
