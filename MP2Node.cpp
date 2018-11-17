/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;
    
	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
    bool has_ring_changed = false;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    for(vector<Node>::iterator it = curMemList.begin();it!=curMemList.end();it++)
    {
        bool is_in_ring= false;
        for(vector<Node>::iterator et = ring.begin();et!=ring.end();et++)
        {
            if((*et).nodeAddress == (*it).nodeAddress)
            {
                is_in_ring = true;
                //std::cout<<"is in ring\n";
                break;
            }
        }
        if(is_in_ring == false){
            ring.emplace_back(*it);
            has_ring_changed = true;
        }
    }
    //Find out nodes that have been deleted
    vector<Node> deletedMemList;
    vector<kvpair> deletedpairList;
    for(vector<Node>::iterator et = ring.begin();et!=ring.end();et++)
    {
        bool is_member = false;
        for(vector<Node>::iterator it = curMemList.begin();it!=curMemList.end();it++)
        {
            if((*et).nodeAddress == (*it).nodeAddress)
            {
                is_member = true;
                break;
            }
        }
        if(is_member == false)
        {
            //add this node to a list of nodes to be deleted
            deletedMemList.emplace_back(*et);
            //std::cout<<"deleted in ring\n";
            has_ring_changed = true;
        }
    }
    //Repair the ring
    for(vector<Node>::iterator et = deletedMemList.begin();et!=deletedMemList.end();et++)
    {
        
        for(vector<Node>::iterator it = ring.begin();it!=ring.end();it++)
        {
            if((*et).nodeAddress == (*it).nodeAddress)
            {
                ring.erase(it);
                break;
            }
        }
    }
    sort(ring.begin(),ring.end());
    //Rehash my current keys
    if(has_ring_changed)
    {
        for(std::map<string,string>::iterator iter = ht->hashTable.begin(); iter != ht->hashTable.end(); ++iter)
        {
           string k =  iter->first;
           clientCreate(iter->first,iter->second);
        }
    }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
    //std::cout<<"My neighbor list is\n";
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
        //std::cout<<addressOfThisMember.getAddress()<<"\n"; 
    }
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
     int dest = hashFunction(key);
     unsigned int i;
     vector<Node> nodesforkey = findNodes(key);
     //std::cout<<"nodes for key size is "<<nodesforkey.size()<<"\n";
     for(i=0;i<nodesforkey.size();i++)
     {
         ReplicaType replica = PRIMARY;
         if(i==1)
         {
             replica = SECONDARY;
         }
         else if(i==2)
         {
             replica = TERTIARY;
         }
         Message m(g_transID,getMemberNode()->addr,CREATE,key,value,replica);    
         emulNet->ENsend(&getMemberNode()->addr, &nodesforkey.at(i).nodeAddress, m.toString());
         //std::cout<<"Sending create from "<<getMemberNode()->addr.getAddress()<<" to "<<nodesforkey.at(i).nodeAddress.getAddress()<<"\n";
     }
     pendingtxn.emplace_back(txnentry(g_transID++,CREATE,0,0,0,key,value));
     
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
     unsigned int i;
     vector<Node> nodesforkey = findNodes(key);
     //std::cout<<"nodes for key size is "<<nodesforkey.size()<<"\n";
     for(i=0;i<nodesforkey.size();i++)
     {
         ReplicaType replica = PRIMARY;
         if(i==1)
         {
             replica = SECONDARY;
         }
         else if(i==2)
         {
             replica = TERTIARY;
         }
         Message m(g_transID,getMemberNode()->addr,READ,key);    
         emulNet->ENsend(&getMemberNode()->addr, &nodesforkey.at(i).nodeAddress, m.toString());
         //std::cout<<"Sending read from "<<getMemberNode()->addr.getAddress()<<" to "<<nodesforkey.at(i).nodeAddress.getAddress()<<"\n";
     }
     string value="NULL";
     pendingtxn.emplace_back(txnentry(g_transID++,READ,0,0,0,key,value));
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
     int dest = hashFunction(key);
     unsigned int i;
     vector<Node> nodesforkey = findNodes(key);
     //std::cout<<"nodes for key size is "<<nodesforkey.size()<<"\n";
     for(i=0;i<nodesforkey.size();i++)
     {
         ReplicaType replica = PRIMARY;
         if(i==1)
         {
             replica = SECONDARY;
         }
         else if(i==2)
         {
             replica = TERTIARY;
         }
         Message m(g_transID,getMemberNode()->addr,UPDATE,key,value,replica);    
         emulNet->ENsend(&getMemberNode()->addr, &nodesforkey.at(i).nodeAddress, m.toString());
         //std::cout<<"Sending update from "<<getMemberNode()->addr.getAddress()<<" to "<<nodesforkey.at(i).nodeAddress.getAddress()<<"\n";
     }
     pendingtxn.emplace_back(txnentry(g_transID++,UPDATE,0,0,0,key,value));
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
      unsigned int i;
     vector<Node> nodesforkey = findNodes(key);
     //std::cout<<"nodes for key size is "<<nodesforkey.size()<<"\n";
     for(i=0;i<nodesforkey.size();i++)
     {
         ReplicaType replica = PRIMARY;
         if(i==1)
         {
             replica = SECONDARY;
         }
         else if(i==2)
         {
             replica = TERTIARY;
         }
         Message m(g_transID,getMemberNode()->addr,DELETE,key);    
         emulNet->ENsend(&getMemberNode()->addr, &nodesforkey.at(i).nodeAddress, m.toString());
         //std::cout<<"Sending delete from "<<getMemberNode()->addr.getAddress()<<" to "<<nodesforkey.at(i).nodeAddress.getAddress()<<"\n";
     }
     string value="NULL";
     pendingtxn.emplace_back(txnentry(g_transID++,DELETE,0,0,0,key,value));
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
    bool op = ht->create(key,value);
    if(op == true)
    {
        // Update replica lists
        if(replica != PRIMARY)
        {
            vector<Node> nodesforkey = findNodes(key);
            //std::cout<<"nodes for key size is "<<nodesforkey.size()<<"\n";
            for(int i=0;i<nodesforkey.size();i++)
            {
                if(!(nodesforkey.at(i).nodeAddress == getMemberNode()->addr))
                {
                    bool isinreplica = false;
                    for(int j=0;j<haveReplicasOf.size();j++)
                    {
                        if(nodesforkey.at(i).nodeAddress == haveReplicasOf.at(j).nodeAddress)
                        {
                            isinreplica = true;
                            break;
                        }
                    }
                    if(isinreplica == false)
                        haveReplicasOf.emplace_back(nodesforkey.at(i));
                }
            }
        }
        else
        {
            vector<Node> nodesforkey = findNodes(key);
            //std::cout<<"nodes for key size is "<<nodesforkey.size()<<"\n";
            for(int i=0;i<nodesforkey.size();i++)
            {
                if(!(nodesforkey.at(i).nodeAddress == getMemberNode()->addr))
                {
                    bool isinreplica = false;
                    for(int j=0;j<hasMyReplicas.size();j++)
                    {
                        if(nodesforkey.at(i).nodeAddress == hasMyReplicas.at(j).nodeAddress)
                        {
                            isinreplica = true;
                            break;
                        }
                    }
                    if(isinreplica == false)
                        hasMyReplicas.emplace_back(nodesforkey.at(i));
                }
            }
        }
    }
    return  op;
    
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
    //string str = "hi";
    //return str;
    return ht->read(key);
    
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
    return ht->update(key,value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
    return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
        Message msg(message);
		/*
		 * Handle the message types here
		 */
        //std::cout<<"Mesg queue of "<<(getMemberNode()->addr).getAddress()<<"\n";
        //std::cout<<"message type "<<msg.type<<"key "<<msg.key<<"value "<<msg.value<<"\n";
        if(msg.type==CREATE)
        {
            bool op = createKeyValue(msg.key,msg.value,msg.replica);
            if(op == true)
            {
                //Log success
                log->logCreateSuccess(&getMemberNode()->addr, false, msg.transID, msg.key, msg.value);
                //Send success message to coordinator
                Message m(msg.transID, getMemberNode()->addr, REPLY, true);
                emulNet->ENsend(&getMemberNode()->addr, &msg.fromAddr, m.toString());
                
            }
            else
            {
                log->logCreateFail(&getMemberNode()->addr, false, msg.transID, msg.key, msg.value);
                //Send failure message to coordinator
                Message m(msg.transID, getMemberNode()->addr, REPLY, false);
                emulNet->ENsend(&getMemberNode()->addr, &msg.fromAddr, m.toString());
            }
        }
        if(msg.type==READ)
        {
            //std::cout<<msg.key<<"count in ht is "<<ht->count(msg.key)<<" \n";
            if(ht->count(msg.key)==0)
            {
                log->logReadFail(&getMemberNode()->addr, false, msg.transID, msg.key);
                string value = "INVALID_KEY";
                Message m(msg.transID, getMemberNode()->addr, value);
                emulNet->ENsend(&getMemberNode()->addr, &msg.fromAddr, m.toString());
            }
            else
            {
                string str = readKey(msg.key);
                log->logReadSuccess(&getMemberNode()->addr,false,msg.transID,msg.key,str);
                Message m(msg.transID, getMemberNode()->addr, str);
                emulNet->ENsend(&getMemberNode()->addr, &msg.fromAddr, m.toString());
            }
            
        }
        if(msg.type==UPDATE)
        {
            bool op = updateKeyValue(msg.key,msg.value,PRIMARY);
            if(op == true)
            {
                //Log success
                log->logUpdateSuccess(&getMemberNode()->addr, false, msg.transID, msg.key, msg.value);
                //Send success message to coordinator
                Message m(msg.transID, getMemberNode()->addr, REPLY, true);
                emulNet->ENsend(&getMemberNode()->addr, &msg.fromAddr, m.toString());
                
            }
            else
            {
                log->logUpdateFail(&getMemberNode()->addr, false, msg.transID, msg.key, msg.value);
                //Send failure message to coordinator
                Message m(msg.transID, getMemberNode()->addr, REPLY, false);
                emulNet->ENsend(&getMemberNode()->addr, &msg.fromAddr, m.toString());
            }
        }
        if(msg.type==DELETE)
        {
            bool op = deletekey(msg.key);
            if(op == true)
            {
                //Log success
                log->logDeleteSuccess(&getMemberNode()->addr, false, msg.transID, msg.key);
                //Send success message to coordinator
                Message m(msg.transID, getMemberNode()->addr, REPLY, true);
                emulNet->ENsend(&getMemberNode()->addr, &msg.fromAddr, m.toString());
                
            }
            else
            {
                log->logDeleteFail(&getMemberNode()->addr, false, msg.transID, msg.key);
                //Send failure message to coordinator
                Message m(msg.transID, getMemberNode()->addr, REPLY, false);
                emulNet->ENsend(&getMemberNode()->addr, &msg.fromAddr, m.toString());
            }
        }
        if(msg.type==REPLY)
        {
            int deltransaction = -1;
            for(int i=0;i<pendingtxn.size();i++)
            {
                if(msg.transID==pendingtxn.at(i).txnid)
                {
                    if(msg.success == true)
                    {
                      pendingtxn.at(i).n_replies++;
                    }
                    else
                    {
                      pendingtxn.at(i).n_fail_replies++;
                    }
                    
                    if(pendingtxn.at(i).n_replies == 2)
                    {
                        if(pendingtxn.at(i).message_type == CREATE)
                        {
                            log->logCreateSuccess(&getMemberNode()->addr, true, msg.transID, pendingtxn.at(i).key, pendingtxn.at(i).value);
                        }
                        else if(pendingtxn.at(i).message_type == UPDATE)
                        {
                            log->logUpdateSuccess(&getMemberNode()->addr, true, msg.transID, pendingtxn.at(i).key, pendingtxn.at(i).value);
                        }
                        else if(pendingtxn.at(i).message_type == DELETE)
                        {
                            log->logDeleteSuccess(&getMemberNode()->addr, true, msg.transID, pendingtxn.at(i).key);
                        }
                        //pendingtxn.erase(pendingtxn.begin()+i);
                        pendingtxn.at(i).complete= true;
                        break;
                    }
                    if(pendingtxn.at(i).n_fail_replies == 2)
                    {
                        if(pendingtxn.at(i).message_type == CREATE)
                        {
                            log->logCreateFail(&getMemberNode()->addr, true, msg.transID, pendingtxn.at(i).key, pendingtxn.at(i).value);
                        }
                        else if(pendingtxn.at(i).message_type == UPDATE)
                        {
                            log->logUpdateFail(&getMemberNode()->addr, true, msg.transID, pendingtxn.at(i).key, pendingtxn.at(i).value);
                        }
                        else if(pendingtxn.at(i).message_type == DELETE)
                        {
                            log->logDeleteFail(&getMemberNode()->addr, true, msg.transID, pendingtxn.at(i).key);
                        }
                        //pendingtxn.erase(pendingtxn.begin()+i);
                        pendingtxn.at(i).complete= true;
                        break;
                    }
                }
            }
        }
        else if(msg.type == READREPLY)
        {
            //cout<<"Getting a read reply "<<msg.value<<"\n";
            for(int i=0;i<pendingtxn.size();i++)
            {
                if(msg.transID==pendingtxn.at(i).txnid)
                {
                    string value = "INVALID_KEY";
                    if(msg.value == value)
                    {
                        //cout<<"Confirm\n";
                        pendingtxn.at(i).n_fail_replies++;
                    }
                    else
                    {   
                        pendingtxn.at(i).n_replies++;
                    }
                    if(pendingtxn.at(i).n_replies == 2)
                    {
                        if(pendingtxn.at(i).message_type == READ)
                        {
                            log->logReadSuccess(&getMemberNode()->addr,true,msg.transID,pendingtxn.at(i).key,msg.value);
                        }
                        //pendingtxn.erase(pendingtxn.begin()+i);
                        pendingtxn.at(i).complete= true;
                        break;
                    }
                    if(pendingtxn.at(i).n_fail_replies == 2)
                    {
                        if(pendingtxn.at(i).message_type == READ)
                        {
                            //log->logReadSuccess(&getMemberNode()->addr,true,msg.transID,pendingtxn.at(i).key,msg.value);
                            log->logReadFail(&getMemberNode()->addr, true, msg.transID, pendingtxn.at(i).key);
                        }
                        //pendingtxn.erase(pendingtxn.begin()+i);
                        pendingtxn.at(i).complete= true;
                        break;
                    }
                }
            }
        }
	}
    for(vector<txnentry>::iterator it = pendingtxn.begin();it != pendingtxn.end();it++)
    {
        (*it).iteration++;
        if((*it).iteration == 3 && (!(*it).complete))
        {
            //Log it as fail and remove it from pending txn
            switch((*it).message_type)
            {
                case READ:
                {
                    log->logReadFail(&getMemberNode()->addr, true, (*it).txnid, (*it).key);
                    break;
                }
                case CREATE:
                {
                    log->logCreateFail(&getMemberNode()->addr, true, (*it).txnid, (*it).key, (*it).value);
                    break;
                }
                case UPDATE:
                {
                    log->logUpdateFail(&getMemberNode()->addr, true, (*it).txnid, (*it).key, (*it).value);
                    break;
                }
                case DELETE:
                {
                    log->logDeleteFail(&getMemberNode()->addr, true, (*it).txnid, (*it).key);
                    break;
                }
                default:break;
            }
            (*it).complete = true;
        }
        
    }
	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
}
