package sputnik.satellite;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

/**
 * RMI remote interface for a sputnik node.
 * TODO: Authentication via "id" is vulnerable to "id guessing" of other callers even with SSL Encryption and Authentication (Server & Client).
 */
public interface Node extends Remote {

	/**
	 * Login at the remote node providing own address so that the remote node can reconnect to the caller node.
	 * @param myHost host address of the caller.
	 * @param myName name of the caller node in its registry.
	 * @return UUID for the caller
	 */
	UUID login(String myHost, String myName, int myPort) throws RemoteException;	
	
	/**
	 * Sends a serialized message to the node.
	 * @param message Byte serialization of a message.
	 */
	void sendMessage(UUID id, byte[] message) throws RemoteException;
	
	/**
	 * Logout at the node.
	 * @param id
	 * @throws RemoteException
	 */
	void logout(UUID id) throws RemoteException;
	
	/**
	 * Pings the node to check whether it is still available.
	 * @throws RemoteException
	 */
	void ping() throws RemoteException;	
}
