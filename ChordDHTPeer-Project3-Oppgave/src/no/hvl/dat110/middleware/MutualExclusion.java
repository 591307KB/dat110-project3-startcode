/**
 * 
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class MutualExclusion {

	/** lock variables */
	private boolean CS_BUSY = false; // indicate to be in critical section (accessing a shared resource)
	private boolean WANTS_TO_ENTER_CS = false; // indicate to want to enter CS
	private List<Message> queueack; // queue for acknowledged messages
	private List<Message> mutexqueue; // queue for storing process that are denied permission. We really don't need
										// this for quorum-protocol

	private LamportClock clock; // lamport clock
	private Node node;

	public MutualExclusion(Node node) throws RemoteException {
		this.node = node;

		clock = new LamportClock();
		queueack = new ArrayList<Message>();
		mutexqueue = new ArrayList<Message>();
	}

	public void acquireLock() {
		CS_BUSY = true;
	}

	public void releaseLocks() {
		WANTS_TO_ENTER_CS = false;
		CS_BUSY = false;
	}

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {

		System.out.println(node.nodename + " wants to access CS");

		queueack.clear();
		mutexqueue.clear();
		clock.increment();
		message.setClock(clock.getClock());
		WANTS_TO_ENTER_CS = true;

		// clear the queueack before requesting for votes

		// clear the mutexqueue

		// increment clock

		// adjust the clock on the message, by calling the setClock on the message

		// wants to access resource - set the appropriate lock variable

		// doDecisionAlgorithm(message,mutexqueue,2);
		List<Message> activenodes = removeDuplicatePeersBeforeVoting();
		multicastMessage(message, activenodes);
		if (areAllMessagesReturned(activenodes.size())) {
			node.acquireLock();
			node.broadcastUpdatetoPeers(updates);
			mutexqueue.clear();
			return true;
		}

		// start MutualExclusion algorithm

		// first, removeDuplicatePeersBeforeVoting. A peer can contain 2 replicas of a
		// file. This peer will appear twice

		// multicast the message to activenodes (hint: use multicastMessage)

		// check that all replicas have replied (permission)

		// if yes, acquireLock

		// node.broadcastUpdatetoPeers

		// clear the mutexqueue

		// return permission

		return false;
	}

	// multicast message to other processes including self
	private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
		for (Message node : activenodes) {
			NodeInterface ni = Util.getProcessStub(node.getNodeIP(), node.getPort());
			ni.onMutexRequestReceived(message);
		}

		// iterate over the activenodes

		// obtain a stub for each node from the registry

		// call onMutexRequestReceived()

	}

	public void onMutexRequestReceived(Message message) throws RemoteException {

		clock.increment();

		if (node.getNodeID().equals(message.getNodeID())) {
			node.onMutexAcknowledgementReceived(message);
		}
		// increment the local clock

		// if message is from self, acknowledge, and call
		// onMutexAcknowledgementReceived()

		int caseid = -1;

		if (!CS_BUSY && !WANTS_TO_ENTER_CS)
			caseid = 0;
		if (CS_BUSY)
			caseid = 1;
		if (!CS_BUSY && WANTS_TO_ENTER_CS)
			caseid = 2;
		// write if statement to transition to the correct caseid
		// caseid=0: Receiver is not accessing shared resource and does not want to
		// (send OK to sender)
		// caseid=1: Receiver already has access to the resource (dont reply but queue
		// the request)
		// caseid=2: Receiver wants to access resource but is yet to - compare own
		// message clock to received message's clock

		// check for decision
		doDecisionAlgorithm(message, mutexqueue, caseid);
	}

	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {

		String procName = message.getNodeIP(); // this is the same as nodeName in the Node class
		int port = message.getPort(); // port on which the registry for this stub is listening

		switch (condition) {

		/**
		 * case 1: Receiver is not accessing shared resource and does not want to (send
		 * OK to sender)
		 */
		case 0: {

			NodeInterface stub = Util.getProcessStub(procName, port);
			stub.onMutexAcknowledgementReceived(message);

			// get a stub for the sender from the registry
			// acknowledge message
			// send acknowledgement back by calling onMutexAcknowledgementReceived()

			break;
		}

		/**
		 * case 2: Receiver already has access to the resource (dont reply but queue the
		 * request)
		 */
		case 1: {
			queue.add(message);
			// queue this message
			break;
		}

		/**
		 * case 3: Receiver wants to access resource but is yet to (compare own message
		 * clock to received message's clock the message with lower timestamp wins) -
		 * send OK if received is lower. Queue message if received is higher
		 */
		case 2: {
			// check the clock of the sending process
			// own clock for the multicast message
			// compare clocks, the lowest wins
			// if clocks are the same, compare nodeIDs, the lowest wins
			// if sender wins, acknowledge the message, obtain a stub and call
			// onMutexAcknowledgementReceived()
			// if sender looses, queue it
			if (clock.getClock() == message.getClock()) {
				if (node.getNodeID().compareTo(message.getNodeID()) < 0) {
					queue.add(message);
				} else {
					NodeInterface stub = Util.getProcessStub(procName, port);
					stub.onMutexAcknowledgementReceived(message);
				}
			} else {
				if (clock.getClock() < message.getClock()) {
					queue.add(message);
				} else {
					NodeInterface stub = Util.getProcessStub(procName, port);
					stub.onMutexAcknowledgementReceived(message);
				}
<<<<<<< HEAD
			}
			break;
=======
				break;
			}
			default: break;
>>>>>>> 0e2bc077ec5694552136eafb9c1eaea4e3f99a3b
		}

		default:
			break;
		}

	}

	public void onMutexAcknowledgementReceived(Message message) throws RemoteException {

		// add message to queueack
		queueack.add(message);

	}

	// multicast release locks message to other processes including self
	public void multicastReleaseLocks(Set<Message> activenodes) {
		
		for (Message node : activenodes) {
			NodeInterface ni = Util.getProcessStub(node.getNodeIP(), node.getPort());
			try {
				ni.releaseLocks();
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}

		// iterate over the activenodes

		// obtain a stub for each node from the registry

		// call releaseLocks()

	}

	private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
		boolean check = false;
		if (queueack.size() == numvoters)
			check = true;
		queueack.clear();

		return check;
		// check if the size of the queueack is same as the numvoters

		// clear the queueack

		// return true if yes and false if no

	}

	private List<Message> removeDuplicatePeersBeforeVoting() {

		List<Message> uniquepeer = new ArrayList<Message>();
		for (Message p : node.activenodesforfile) {
			boolean found = false;
			for (Message p1 : uniquepeer) {
				if (p.getNodeIP().equals(p1.getNodeIP())) {
					found = true;
					break;
				}
			}
			if (!found)
				uniquepeer.add(p);
		}
		return uniquepeer;
	}
}
