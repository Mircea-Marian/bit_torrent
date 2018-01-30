import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;




public class Client {
	private final String clientHost;
	private final int clientPort;
	private final String serverHost;
	private final int serverPort;
	private int chunkSize = 1024;

	private final TreeMap<String, TreeMap<Integer, byte[]>> inMemoryFiles;
		// Matrice in care se tin in memorie fragmente de la fisiere downloadate incomplet.

	private final Semaphore filesSem;
	private Thread sharingThread;
	private final AtomicBoolean isRunning;
	private final Selector serverSelector;
	private final String homeDir;

	public Client(String clientHostt, int clientPortt, String serverHost, int serverPort, String hD){
		this.homeDir = hD;
			// Folderul in care se downloadeaza si se uploadeaza fisiere

		this.clientHost = clientHostt;
		this.clientPort = clientPortt;
		this.serverHost = serverHost;
		this.serverPort = serverPort;
		this.isRunning = new AtomicBoolean(true);
		this.inMemoryFiles = new TreeMap<>();
		this.filesSem = new Semaphore(1);
		Selector newSel = null;
		try {
			newSel = Selector.open();
		} catch (IOException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		}
		this.serverSelector = newSel;

		Runnable thingy = new Runnable() {

			@Override
			public void run() {
				// TODO Auto-generated method stub
				try {
					// Se initializeaza canalul pe care o sa se asculte pentru conexiuni de la alti clienti.
					final ServerSocketChannel serverChannel = ServerSocketChannel.open();
					serverChannel.configureBlocking(false);
					serverChannel.socket().bind(new InetSocketAddress(clientHost, clientPort));
					serverChannel.register(serverSelector, SelectionKey.OP_ACCEPT);

					ExecutorService executor = Executors.newFixedThreadPool(5);

					while(isRunning.get()){

						// Se obtin evenimentele curente.
						try {
							if (serverSelector.select() <= 0) {
									continue;
							}
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

						// Se itereaza prin lista de canale si se reactioneaza la evenimente.
						Iterator iterator = serverSelector.selectedKeys().iterator();
						while (iterator.hasNext()) {
							final SelectionKey key = (SelectionKey) iterator.next();

							if(key.isAcceptable()) {
								try {
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

											// Se citesc denumirea fisierului si id-ul fragmentului.
											buffer.flip();
											buffer.getShort();
											short k = buffer.getShort();
											StringBuffer fileNameBuffer = new StringBuffer(k);
											for(int i = 0 ; i < k ; i++){
												char ch = buffer.getChar();
												fileNameBuffer.append(ch);
											}
											int fragNo = buffer.getInt();

											byte[] fragmentData = null;

											try {
												filesSem.acquire();
											} catch (InterruptedException e) {
												// TODO Auto-generated catch block
												e.printStackTrace();
											}

											// Se verifica daca fragmentul cerut nu este deja in memorie.
											if(inMemoryFiles.containsKey(fileNameBuffer.toString())){
												TreeMap<Integer, byte[]> fragData = inMemoryFiles.get(fileNameBuffer.toString());
												if(!fragData.containsKey(fragNo))
													fragmentData = fragData.get(fragNo);
											}

											filesSem.release();

											// Daca nu s-a gasit in memorie fragmentul, atunci se cauta in directorul clientului.
											if(fragmentData == null){
												File file = new File(homeDir + fileNameBuffer.toString().substring(1));
												if(!file.exists() || file.isDirectory()){
													try {
														channel.write(ByteBuffer.allocate(4).putInt(-1));
														channel.close();
													} catch (IOException e) {
														// TODO Auto-generated catch block
														e.printStackTrace();
													}
													return;
												}
												try {
													long fileDim = file.length();
													int numberOfFrags = (int)((fileDim%chunkSize == 0)?fileDim/chunkSize:(fileDim/chunkSize+1));
													int fragSize;
													if(fragNo == numberOfFrags - 1 && fileDim%chunkSize != 0){
														fragSize = (int)fileDim%chunkSize;
													} else
														fragSize = chunkSize;
													fragmentData = Arrays.copyOfRange(
														Files.readAllBytes(Paths.get(homeDir + fileNameBuffer.toString().substring(1))),
														fragNo * chunkSize,
														fragNo * chunkSize + fragSize
													);

													// Se trimite fragmentul.
													ByteBuffer deTrim = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE + fragSize);
													deTrim.putInt(fragSize);
													deTrim.put(fragmentData);
													deTrim.flip();

													while(deTrim.hasRemaining())
														channel.write(deTrim);
												} catch (IOException e) {
													// TODO Auto-generated catch block
													e.printStackTrace();
												}
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
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};

		this.sharingThread = new Thread(thingy);
		this.sharingThread.start();
	}

	public void publishFile(File file) throws IOException {

		// Se fragmenteaza fisierul.
		long fileSize = file.length(), cFileSize = fileSize;


		Socket serverSocket = new Socket(this.serverHost, this.serverPort);

		short buffDim = (short)((
				Short.SIZE * 3
				+ Integer.SIZE * 3
				+ Character.SIZE * (file.getName().length() + 1 + "127.0.0.1".length() + 1)
				+ Byte.SIZE * 1
		)/Byte.SIZE);

		ByteBuffer deTrim = ByteBuffer.allocate(buffDim);

		deTrim.putShort(buffDim);

		deTrim.put((byte)0);

		// Se transmite numele fisierului.
		deTrim.putShort( (short)(file.getName().getBytes(StandardCharsets.UTF_16).length/2) );
		deTrim.put(file.getName().getBytes(StandardCharsets.UTF_16));

		deTrim.putInt((int)cFileSize);

		deTrim.putInt(chunkSize);

		// Se trimit  datele despre conectarea la client.
		deTrim.putShort( (short)(this.clientHost.getBytes(StandardCharsets.UTF_16).length/2) );
		deTrim.put(this.clientHost.getBytes(StandardCharsets.UTF_16));

		deTrim.putInt(this.clientPort);

		serverSocket.getOutputStream().write(deTrim.array());

		serverSocket.close();
	}

	public File retrieveFile(String filename) throws IOException {
		final Socket serverSocket = new Socket(this.serverHost, this.serverPort);

		short buffDim = (short)((
				Short.SIZE * 2
				+ Byte.SIZE * 1
				+ Character.SIZE * (filename.length() + 1)
		)/Byte.SIZE);

		ByteBuffer deTrim = ByteBuffer.allocate(buffDim);

		deTrim.putShort(buffDim);

		deTrim.put((byte)1);

		deTrim.putShort( (short)(filename.getBytes(StandardCharsets.UTF_16).length/2) );
		deTrim.put(filename.getBytes(StandardCharsets.UTF_16));

		serverSocket.getOutputStream().write(deTrim.array());

		DataInputStream dis = new DataInputStream(serverSocket.getInputStream());

		int messSize = dis.readInt();
		int fileSize = dis.readInt();
		byte[] dataByteArr = new byte [messSize-(2 * Integer.SIZE/Byte.SIZE)];
		dis.readFully(dataByteArr);

		dis.close();
		serverSocket.close();

		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(dataByteArr));
		final TreeMap<Integer, ArrayList<Server.ClientAddress>> fragsInfo;
		try {
			// Se incearca citirea informatiilor despre fragmente.
			fragsInfo = (TreeMap<Integer, ArrayList<Server.ClientAddress>>) ois.readObject();

			if(fragsInfo.size() == 0)
				return null;

			final File fileToGet = new File(this.homeDir + filename);

			ExecutorService executor = Executors.newFixedThreadPool(5);

			for (Map.Entry<Integer, ArrayList<Server.ClientAddress>> entry : fragsInfo.entrySet()){
				final Map.Entry<Integer, ArrayList<Server.ClientAddress>> entryCpy = entry;
				final String cFilename = filename;
				executor.execute(
						new Runnable() {
							@Override
							public void run() {
								// TODO Auto-generated method stub
								Socket peerSocket = new Socket();
								boolean foundPeerFlag = false;

								// Se conecteaza la clientul care raspunde cel mai repede la cerere.
								while(!foundPeerFlag){
									for(Server.ClientAddress cA : entryCpy.getValue()){
										try {
											peerSocket.connect(new InetSocketAddress(cA.ipAddress.substring(1), cA.port), 2000);
											foundPeerFlag = true;
										} catch (SocketTimeoutException ste){
											continue;
										}
										catch (IOException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
										break;
									}
								}

								short buffDim = (short)((
										Short.SIZE * 2
										+ Character.SIZE * (cFilename.length() + 1)
										+ Integer.SIZE * 1
								)/Byte.SIZE);

								// Se trimite cererea pentru fragment.
								ByteBuffer deTrim = ByteBuffer.allocate(buffDim);
								deTrim.putShort(buffDim);
								deTrim.putShort( (short)(cFilename.getBytes(StandardCharsets.UTF_16).length/2) );
								deTrim.put(cFilename.getBytes(StandardCharsets.UTF_16));
								deTrim.putInt(entryCpy.getKey());

								try {
									peerSocket.getOutputStream().write(deTrim.array());
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

								DataInputStream dis = null;
								try {
									dis = new DataInputStream(peerSocket.getInputStream());

									// Se citeste informatia despre fragment.
									int fragSize = dis.readInt();
									if(fragSize != -1){
										byte[] fragmentDataArr = new byte [fragSize];
										dis.readFully(fragmentDataArr);

										filesSem.acquire();

										if(inMemoryFiles.containsKey(cFilename)){
											TreeMap<Integer, byte[]> fragData = inMemoryFiles.get(cFilename);
											if(!fragData.containsKey(entryCpy.getKey()))
												fragData.put(entryCpy.getKey(), fragmentDataArr);
										} else {
											TreeMap<Integer, byte[]> fragData = new TreeMap<>();
											fragData.put(entryCpy.getKey(), fragmentDataArr);
											inMemoryFiles.put(cFilename, fragData);
										}

										filesSem.release();
										dis.close();
										peerSocket.close();

										Socket serverSocket = new Socket(serverHost, serverPort);

										buffDim = (short)((
												Short.SIZE * 3
												+ Character.SIZE * (cFilename.length() + 1 + clientHost.length() + 1)
												+ Integer.SIZE * 2
												+ Byte.SIZE * 1
										)/Byte.SIZE);

										deTrim = ByteBuffer.allocate(buffDim);

										// Se anunta serverul ca fragmentul este gata de download daca alti clienti il vor,
										// desii nu s-a terminar download-ul complet al fisierului.
										deTrim.putShort(buffDim);
										deTrim.put((byte)2);
										deTrim.putShort( (short)(cFilename.getBytes(StandardCharsets.UTF_16).length/2) );
										deTrim.put(cFilename.getBytes(StandardCharsets.UTF_16));
										deTrim.putInt(entryCpy.getKey());
										deTrim.putShort( (short)(clientHost.getBytes(StandardCharsets.UTF_16).length/2) );
										deTrim.put(clientHost.getBytes(StandardCharsets.UTF_16));
										deTrim.putInt(clientPort);

										serverSocket.getOutputStream().write(deTrim.array());
									} else {
										dis.close();
										peerSocket.close();
									}

									serverSocket.close();
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
						}
				);
			}

			executor.shutdown();
			try {
				executor.awaitTermination(5, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			//Se scrie fisierul.
			try {
				filesSem.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			TreeMap<Integer, byte[]> fragData = inMemoryFiles.get(filename);

			filesSem.release();

			FileOutputStream fos = new FileOutputStream(fileToGet);

			int numberOfFrags = (fileSize%chunkSize==0)?fileSize/chunkSize:fileSize/chunkSize+1;
			int lastFragID = -1;
			for (Map.Entry<Integer, byte[]> entry : fragData.entrySet()){
				fos.write(entry.getValue());
			}
			fos.close();

			try {
				filesSem.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			inMemoryFiles.remove(filename);

			filesSem.release();

			return fileToGet;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public void startAndAwaitTerminaton(){
		this.isRunning.set(false);
		this.serverSelector.wakeup();
		try {
			this.sharingThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Client client = new Client("localhost", Integer.parseInt(args[0]), args[1], Integer.parseInt(args[2]), "./");

		//TODO: Se pun dupa aces comentariu operatiile pe care acest client ar trebui sa le faca.

		String line;
		Scanner scanner = new Scanner(System.in);

		while(true){
			line = scanner.nextLine().trim();
			if(line.startsWith("exit")){
				scanner.close();
				break;
			}
		}

		client.startAndAwaitTerminaton();

		// Bucata de cod comentata de mai jos a fost folosita la testarea functionalitatii
		// sistemului.

		// Client client1 = new Client("localhost", 4001, "localhost", 4000, "./");
		// Client client2 = new Client("localhost", 4002, "localhost", 4000, "./client2/");
		// try {
		// 	client1.publishFile(new File("comenzi"));

		// 	try {
		// 		Thread.sleep(2000);
		// 	} catch (InterruptedException e) {
		// 		// TODO Auto-generated catch block
		// 		e.printStackTrace();
		// 	}


		// 	client2.retrieveFile("comenzi");

		// 	client1.startAndAwaitTerminaton();
		// 	client2.startAndAwaitTerminaton();
		// } catch (IOException e) {
		// 	// TODO Auto-generated catch block
		// 	e.printStackTrace();
		// }

	}
}
