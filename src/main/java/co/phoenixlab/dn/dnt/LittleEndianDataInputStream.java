/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Vincent Zhang/PhoenixLAB
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package co.phoenixlab.dn.dnt;

import java.io.*;

public class LittleEndianDataInputStream extends FilterInputStream implements DataInput {

    private final DataInputStream parent;

    public LittleEndianDataInputStream(DataInputStream in) {
        super(in);
        parent = in;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        parent.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        parent.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return parent.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return parent.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return parent.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return parent.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return Short.reverseBytes(parent.readShort());
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return Short.toUnsignedInt(readShort());
    }

    @Override
    public char readChar() throws IOException {
        return Character.reverseBytes(parent.readChar());
    }

    @Override
    public int readInt() throws IOException {
        return Integer.reverseBytes(parent.readInt());
    }

    public long readUnsignedInt() throws IOException {
        return Integer.toUnsignedLong(readInt());
    }

    @Override
    public long readLong() throws IOException {
        return Long.reverseBytes(parent.readLong());
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readLine() throws IOException {
        return parent.readLine();
    }

    @Override
    public String readUTF() throws IOException {
        return parent.readUTF();
    }
}
