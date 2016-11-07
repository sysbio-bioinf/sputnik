/* Copyright (c) Gunnar Völkel. All rights reserved.
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v1.0.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 * You must not remove this notice, or any other, from this software.
 */

package sputnik.satellite.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;


public class MessageBufferInputStream extends InputStream {
		
	private LinkedList<byte[]> receivedByteArrays = new LinkedList<byte[]>();
		
	private long receivedByteCount = 0;
	private long readByteCount = 0;
	
	private int currentPosition = 0;
	private byte[] currentBytes = null;
	
	private int remainingMessageLength = 0;
	
	public MessageBufferInputStream() { }
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Object clone() throws CloneNotSupportedException {
		MessageBufferInputStream clone = new MessageBufferInputStream();
		
		// cloning the list suffices since the byte arrays will not be manipulated
		clone.receivedByteArrays = (LinkedList<byte[]>) receivedByteArrays.clone();
		clone.readByteCount = receivedByteCount;
		clone.readByteCount = readByteCount;
		clone.currentPosition = currentPosition;
		clone.currentBytes = currentBytes;
		clone.remainingMessageLength = remainingMessageLength;
		
		return clone;
	}
	
	
	public void add(byte[] b){
		receivedByteArrays.add(b);
		receivedByteCount += b.length;		
	}
	
	
	public void add(byte[] b, int off, int len){
		int copyLength = Math.min( len, b.length - off);
		
		byte[] bytes = new byte[ copyLength ];
		System.arraycopy( b, off, bytes, 0, copyLength );
		
		add( bytes );
	}
	
	
	public void setNextMessageLength(int messageLength) {
		remainingMessageLength = messageLength;
	}
	
	
	@Override
	public int available() throws IOException {
		return (int)(receivedByteCount - readByteCount);
	}
	
	
	private void ensureBytes() {
		// if current byte array is already read completely
		if( currentBytes != null && currentPosition >= currentBytes.length )
			currentBytes = null;
		
		if( currentBytes == null ){
		  currentBytes = receivedByteArrays.removeFirst();
		  currentPosition = 0;
		}
	}
	

	@Override
	public int read() throws IOException {
		ensureBytes();
		readByteCount++;
		return currentBytes[ currentPosition++ ];
	}

	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int read = 0;
		
		int toRead = Math.min( len, available() );
		
		// limit read to remaining message length if specified
		if( remainingMessageLength > 0)
			toRead = Math.min( toRead, remainingMessageLength );
		else
			return -1;
		
		while( read < toRead ){
			ensureBytes();
			// try to read the missing bytes but read at most the number of available bytes of the currentBytes array
			int currentRead = Math.min( toRead - read, currentBytes.length - currentPosition );
			System.arraycopy( currentBytes, currentPosition, b, off, currentRead );
			
			currentPosition += currentRead;
			off += currentRead;
			read += currentRead;
		}
		
		remainingMessageLength -= read;		
		readByteCount += read;
		return read;
	}
	
	
	@Override
	public long skip(long arg0) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mark(int readlimit) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	@Override
	public void reset() throws IOException {}
	
	@Override
	public void close() throws IOException {}
}