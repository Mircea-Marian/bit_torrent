import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;




public class Server {
	static class ClientAddress implements Comparable<ClientAddress>, Serializable{
		public String ipAddress;
		public int port;

		public ClientAddress(String s, int n) {
			this.ipAddress = s;
			this.port = n;
		}

		@Override
		public int compareTo(ClientAddress arg0) {
			// TODO Auto-generated method stub
			if(this.ipAddress.compareTo(arg0.ipAddress) == 0 && this.port == arg0.port)
				return 0;
			if(this.ipAddress.compareTo(arg0.ipAddress) == 0)
				return this.port - arg0.port;
			return this.ipAddress.compareTo(arg0.ipAddress);
		}
	}
	static class FileInfo {
		public String name;
		public HashSet<Integer> ownedFrags;

		public FileInfo(String name){
			this.name = name;
			this.ownedFrags = new HashSet<>();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			final Selector serverSelector = Selector.open();
			final ServerSocketChannel serverChannel = ServerSocketChannel.open();
			serverChannel.configureBlocking(false);
			serverChannel.socket().bind(new InetSocketAddress("127.0.0.1", Integer.parseInt(args[0])));
			serverChannel.register(serverSelector, SelectionKey.OP_ACCEPT);

			final AtomicBoolean isRunning = new AtomicBoolean(true);

			Thread threadPoolManager = new Thread() {
				public void run() {
					ExecutorService executor = Executors.newFixedThreadPool(5);

					final TreeMap<ClientAddress, ArrayList<FileInfo>> filesInfo = new TreeMap<>();
					final Semaphore filesInfoSem = new Semaphore(1);

					while(isRunning.get()){

						// Se asculta pentru evenimente.
						try {
							if (serverSelector.select() <= 0) {
									continue;
							}
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

						Iterator iterator = serverSelector.selectedKeys().iterator();
						while (iterator.hasNext()) {
							final SelectionKey key = (SelectionKey) iterator.next();

							if(key.isAcceptable()) {
								try {
										// Se accepta o conexiune de la un client.
										ServerSocketChannel ssChannel = (ServerSocketChannel) key.channel();
										SocketChannel sChannel = (SocketChannel) ssChannel.accept();

										sChannel.configureBlocking(false);
										sChannel.register(key.selector(), SelectionKey.OP_READ);

									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
							}

							if(key.isReadable()) {
								key.cancel();
								executor.execute(
									new Runnable() {
										@Override
										public void run() {
											// TODO Auto-generated method stub

											SocketChannel channel = (SocketChannel) key.channel();
											ByteBuffer buffer = ByteBuffer.allocate(8192);
											int numRead = -1;
											int totalRead = 0;

											// Se citeste mesajul de la client.
											try {
										        	numRead = channel.read(buffer);
										        	totalRead += numRead;
										        }
										    catch (IOException e) {
										            e.printStackTrace();
										    }

											while(buffer.getShort(0) < totalRead) {
										        try {
										        	numRead = channel.read(buffer);
										        	totalRead += numRead;
										        }
										        catch (IOException e) {
										            e.printStackTrace();
										        }
											}

											buffer.flip();
											buffer.getShort();
											byte opType = buffer.get();
												// Codurile operatiilor sunt:
												// 0 -> Clientul vrea sa publice fisierul.
												// 1 -> Clientul vrea sa ia informatii despre un fisier.
												// 2 -> Clientul anunta ca are un fragment dintr-un fisier.
											if(opType == 0) {
												short k = buffer.getShort();
												StringBuffer fileNameBuffer = new StringBuffer(k);
												for(int i = 0 ; i < k ; i++){
													char ch = buffer.getChar();
													fileNameBuffer.append(ch);
												}
												int fileSize = buffer.getInt();
												int firstFragSize = buffer.getInt();
												k = buffer.getShort();
												StringBuffer ipAddrBuffer = new StringBuffer(k);
												for(int i = 0 ; i < k ; i++){
													char ch = buffer.getChar();
													ipAddrBuffer.append(ch);
												}
												int port = buffer.getInt();

												int numberOfFragments = (fileSize%firstFragSize == 0) ? fileSize/firstFragSize : fileSize/firstFragSize + 1;
												FileInfo fI = new FileInfo(fileNameBuffer.toString());
												for(int i = 0 ; i < numberOfFragments ; i++)
													fI.ownedFrags.add(i);

												try {
													filesInfoSem.acquire();
												} catch (InterruptedException e1) {
													// TODO Auto-generated catch block
													e1.printStackTrace();
												}

												// Se introduc informatiile despre fisier in memorie.
												if(!filesInfo.containsKey(new ClientAddress(ipAddrBuffer.toString(), port))){
													ArrayList<FileInfo> arr = new ArrayList<>();
													arr.add(fI);
													filesInfo.put(new ClientAddress(ipAddrBuffer.toString(), port), arr);
												} else
													filesInfo.get(new ClientAddress(ipAddrBuffer.toString(), port)).add(fI);

												filesInfoSem.release();
											} else if(opType == 1){

												short k = buffer.getShort();
												StringBuffer fileNameBuffer = new StringBuffer(k);
												for(int i = 0 ; i < k ; i++){
													char ch = buffer.getChar();
													fileNameBuffer.append(ch);
												}

												TreeMap<Integer, ArrayList<ClientAddress>> fragsInfo = new TreeMap<>();

												try {
													filesInfoSem.acquire();
												} catch (InterruptedException e1) {
													// TODO Auto-generated catch block
													e1.printStackTrace();
												}

												for (Map.Entry<ClientAddress, ArrayList<FileInfo>> entry : filesInfo.entrySet()){
													for(int i = 0 ; i < entry.getValue().size() ; i ++){
														FileInfo fI = entry.getValue().get(i);
														if(fI.name.compareTo(fileNameBuffer.toString()) == 0){
															for(Integer fID : fI.ownedFrags){
																if(fragsInfo.containsKey(fID))
																	fragsInfo.get(fID).add(entry.getKey());
																else {
																	ArrayList<ClientAddress> arr = new ArrayList<>();
																	arr.add(entry.getKey());
																	fragsInfo.put(fID, arr);
																}
															}
															break;
														}
													}
												}
												filesInfoSem.release();

												// Se trimit informatiile despre fragmente.
												ByteArrayOutputStream baos = new ByteArrayOutputStream();
												for(int i=0;i<4;i++) baos.write(0);
												try {
													baos.write(ByteBuffer.allocate(4).putInt(0).array());
												} catch (IOException e1) {
													// TODO Auto-generated catch block
													e1.printStackTrace();
												}
												ObjectOutputStream oos;
												try {
													oos = new ObjectOutputStream(baos);
													oos.writeObject(fragsInfo);
												    oos.close();
												    final ByteBuffer wrap = ByteBuffer.wrap(baos.toByteArray());
												    int objDim = baos.size();
												    wrap.putInt(0, baos.size());
												    while(wrap.hasRemaining())
												    	channel.write(wrap);
												} catch (IOException e) {
													// TODO Auto-generated catch block
													e.printStackTrace();
												}
											} else {
												short k = buffer.getShort();
												StringBuffer fileNameBuffer = new StringBuffer(k);
												for(int i = 0 ; i < k ; i++){
													char ch = buffer.getChar();
													fileNameBuffer.append(ch);
												}

												int fID = buffer.getInt();

												k = buffer.getShort();
												StringBuffer ipAddrBuffer = new StringBuffer(k);
												for(int i = 0 ; i < k ; i++){
													char ch = buffer.getChar();
													ipAddrBuffer.append(ch);
												}
												int port = buffer.getInt();

												try {
													filesInfoSem.acquire();
												} catch (InterruptedException e1) {
													// TODO Auto-generated catch block
													e1.printStackTrace();
												}

												if(filesInfo.containsKey(new ClientAddress(ipAddrBuffer.toString(), port))){
													ArrayList<FileInfo> clientFiles = filesInfo.get(new ClientAddress(ipAddrBuffer.toString(), port));
													boolean foundFile = false;
													for(FileInfo fI : clientFiles){
														if(fI.name.compareTo(fileNameBuffer.toString()) == 0){
															fI.ownedFrags.add(fID);
															foundFile = true;
															break;
														}
													}
													if(!foundFile){
														FileInfo fI = new FileInfo(fileNameBuffer.toString());
														fI.ownedFrags.add(fID);
														clientFiles.add(fI);
													}
												} else {
													ArrayList<FileInfo> clientFiles = new ArrayList<>();
													FileInfo fI = new FileInfo(fileNameBuffer.toString());
													fI.ownedFrags.add(fID);
													clientFiles.add(fI);
													filesInfo.put(new ClientAddress(ipAddrBuffer.toString(), port), clientFiles);
												}

												filesInfoSem.release();
											}

									        try {
												channel.close();
											} catch (IOException e) {
												// TODO Auto-generated catch block
												e.printStackTrace();
											}
										}
									}
								);
							}

							iterator.remove();
						}
					}
					executor.shutdown();
					try {
						executor.awaitTermination(5, TimeUnit.MINUTES);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};
			threadPoolManager.start();

			String line;
			Scanner scanner = new Scanner(System.in);

			while(true){
				line = scanner.nextLine().trim();
				if(line.startsWith("exit")){
					scanner.close();
					break;
				}
			}

			isRunning.set(false);
			serverSelector.wakeup();
			try {
				threadPoolManager.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Iterator iterator = serverSelector.selectedKeys().iterator();
			while (iterator.hasNext()) {

				final SelectionKey key = (SelectionKey) iterator.next();
				iterator.remove();

				if(key.isReadable())
					key.channel().close();
			}
			serverChannel.close();
			serverSelector.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
