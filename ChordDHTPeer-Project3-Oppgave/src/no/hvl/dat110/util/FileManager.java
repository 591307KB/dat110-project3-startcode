package no.hvl.dat110.util;

/**
 * @author tdoy
 * dat110 - project 3
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Hash;

public class FileManager {

	private BigInteger[] replicafiles; // array stores replicated files for distribution to matching nodes
	private int numReplicas; // let's assume each node manages nfiles (5 for now) - can be changed from the
								// constructor
	private NodeInterface chordnode;
	private String filepath; // absolute filepath
	private String filename; // only filename without path and extension
	private BigInteger hash;
	private byte[] bytesOfFile;
	private String sizeOfByte;

	private Set<Message> activeNodesforFile = null;

	public FileManager(NodeInterface chordnode) throws RemoteException {
		this.chordnode = chordnode;
	}

	public FileManager(NodeInterface chordnode, int N) throws RemoteException {
		this.numReplicas = N;
		replicafiles = new BigInteger[N];
		this.chordnode = chordnode;
	}

	public FileManager(NodeInterface chordnode, String filepath, int N) throws RemoteException {
		this.filepath = filepath;
		this.numReplicas = N;
		replicafiles = new BigInteger[N];
		this.chordnode = chordnode;
	}

	public void createReplicaFiles() {

		for (int i = 0; i < numReplicas; i++) {
			String s = filename.concat("" + i);
			hash = Hash.hashOf(s);
			replicafiles[i] = hash;
		}

		// implement

		// set a loop where size = numReplicas

		// replicate by adding the index to filename

		// hash the replica

		// store the hash in the replicafiles array.

	}

	/**
	 * 
	 * @param bytesOfFile
	 * @throws RemoteException
	 */
	public int distributeReplicastoPeers() throws RemoteException {
		int counter = 0;

		createReplicaFiles();
		Random rnd = new Random();
		int index = rnd.nextInt(Util.numReplicas - 1);
		for (BigInteger x : replicafiles) {
			NodeInterface n = chordnode.findSuccessor(x);
			n.addKey(x);

			if (counter == index)
				n.saveFileContent(filename, x, bytesOfFile, true);
			else
				n.saveFileContent(filename, x, bytesOfFile, false);
			counter++;
		}

		// server at index=counter will be our primary for this file

		// Task1: Given a filename, make replicas and distribute them to all active
		// peers such that: pred < replica <= peer

		// Task2: assign a replica as the primary for this file. Hint, see the slide
		// (project 3) on Canvas

		// create replicas of the filename

		// iterate over the replicas

		// for each replica, find its successor by performing findSuccessor(replica)

		// call the addKey on the successor and add the replica

		// call the saveFileContent() on the successor

		// increment counter

		return counter;
	}

	/**
	 * 
	 * @param filename
	 * @return list of active nodes having the replicas of this file
	 * @throws RemoteException
	 */
	public Set<Message> requestActiveNodesForFile(String filename) throws RemoteException {

		this.filename = filename;
		Set<Message> succinfo = new HashSet<Message>();

		createReplicaFiles();
		
		for(BigInteger b : replicafiles) {
			NodeInterface s = chordnode.findSuccessor(b);
			succinfo.add(s.getFilesMetadata().get(b));
		}
		// Task: Given a filename, find all the peers that hold a copy of this file

		// generate the N replicas from the filename by calling createReplicaFiles()

		// it means, iterate over the replicas of the file

		// for each replica, do findSuccessor(replica) that returns successor s.

		// get the metadata (Message) of the replica from the successor, s (i.e. active
		// peer) of the file

		// save the metadata in the set succinfo.

		this.activeNodesforFile = succinfo;

		return succinfo;
	}

	/**
	 * Find the primary server - Remote-Write Protocol
	 * 
	 * @return
	 */
	public NodeInterface findPrimaryOfItem() {

		for(Message n : activeNodesforFile) {
			if(n.isPrimaryServer()){
				try {
					return chordnode.findSuccessor(n.getNodeID());
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				
			}
		}
		// Task: Given all the active peers of a file (activeNodesforFile()), find which
		// is holding the primary copy

		// iterate over the activeNodesforFile

		// for each active peer (saved as Message)

		// use the primaryServer boolean variable contained in the Message class to
		// check if it is the primary or not

		// return the primary

		return null;
	}

	/**
	 * Read the content of a file and return the bytes
	 * 
	 * @throws IOException
	 * @throws NoSuchAlgorithmException
	 */
	public void readFile() throws IOException, NoSuchAlgorithmException {

		File f = new File(filepath);

		byte[] bytesOfFile = new byte[(int) f.length()];

		FileInputStream fis = new FileInputStream(f);

		fis.read(bytesOfFile);
		fis.close();

		// set the values
		filename = f.getName().replace(".txt", "");
		hash = Hash.hashOf(filename);
		this.bytesOfFile = bytesOfFile;
		double size = (double) bytesOfFile.length / 1000;
		NumberFormat nf = new DecimalFormat();
		nf.setMaximumFractionDigits(3);
		sizeOfByte = nf.format(size);

		System.out.println("filename=" + filename + " size=" + sizeOfByte);

	}

	public void printActivePeers() {

		activeNodesforFile.forEach(m -> {
			String peer = m.getNodeIP();
			String id = m.getNodeID().toString();
			String name = m.getNameOfFile();
			String hash = m.getHashOfFile().toString();
			int size = m.getBytesOfFile().length;

			System.out.println(
					peer + ": ID = " + id + " | filename = " + name + " | HashOfFile = " + hash + " | size =" + size);

		});
	}

	/**
	 * @return the numReplicas
	 */
	public int getNumReplicas() {
		return numReplicas;
	}

	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}

	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}

	/**
	 * @return the hash
	 */
	public BigInteger getHash() {
		return hash;
	}

	/**
	 * @param hash the hash to set
	 */
	public void setHash(BigInteger hash) {
		this.hash = hash;
	}

	/**
	 * @return the bytesOfFile
	 */
	public byte[] getBytesOfFile() {
		return bytesOfFile;
	}

	/**
	 * @param bytesOfFile the bytesOfFile to set
	 */
	public void setBytesOfFile(byte[] bytesOfFile) {
		this.bytesOfFile = bytesOfFile;
	}

	/**
	 * @return the size
	 */
	public String getSizeOfByte() {
		return sizeOfByte;
	}

	/**
	 * @param size the size to set
	 */
	public void setSizeOfByte(String sizeOfByte) {
		this.sizeOfByte = sizeOfByte;
	}

	/**
	 * @return the chordnode
	 */
	public NodeInterface getChordnode() {
		return chordnode;
	}

	/**
	 * @return the activeNodesforFile
	 */
	public Set<Message> getActiveNodesforFile() {
		return activeNodesforFile;
	}

	/**
	 * @return the replicafiles
	 */
	public BigInteger[] getReplicafiles() {
		return replicafiles;
	}

	/**
	 * @param filepath the filepath to set
	 */
	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
}
