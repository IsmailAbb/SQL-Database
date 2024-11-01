package database.java;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class BPlusTree<T extends Comparable<T>> implements Serializable {
    private static final long serialVersionUID = 1L;

    private Node<T> root;
    private int degree;
    private Class<?> keyType;

    public BPlusTree(int degree, Class<?> keyType) {
        if (!(keyType == String.class || keyType == Integer.class || keyType == Double.class)) {
            throw new IllegalArgumentException("Key type must be String, Integer, or Double.");
        }
        this.degree = degree;
        this.keyType = keyType;
        this.root = new LeafNode<>();
    }

    public List<String> search(T key) {
        return root.search(key);
    }

    public void insert(T key, String value) {
        if (key.getClass() != keyType) {
            throw new IllegalArgumentException("Key type mismatch.");
        }
        root.insert(key, value);
        if (root.isOverflow()) {
            Node<T> newRoot = new InternalNode<>();
            ((InternalNode<T>) newRoot).children.add(root);
            root.split(newRoot, 0);
            root = newRoot;
        }
        // Update root if needed
        while (!root.isLeaf() && root.isOverflow()) {
            Node<T> newRoot = new InternalNode<>();
            ((InternalNode<T>) newRoot).children.add(root);
            root.split(newRoot, 0);
            root = newRoot;
        }
    }


    /*public void delete(T key, String value) {
        root.delete(key, value);
        if (!root.isLeaf() && ((InternalNode<T>) root).keys.isEmpty()) {
            root = ((InternalNode<T>) root).children.get(0);
        }
    }*/

    private interface Node<T extends Comparable<T>> extends Serializable {
        List<String> search(T key);
        void insert(T key, String value);
        void delete(T key, String value);
        boolean isOverflow();
        boolean isLeaf();
        void split(Node<T> parent, int index);
        int binarySearch(List<T> list, T key);
    }

    private class LeafNode<T extends Comparable<T>> implements Node<T> {
        List<T> keys;
        List<List<String>> values;

        LeafNode() {
            this.keys = new ArrayList<>();
            this.values = new ArrayList<>();
        }

        @Override
        public List<String> search(T key) {
            int index = binarySearch(keys, key);
            if (index >= 0) {
                return values.get(index);
            }
            return null;
        }

        @Override
        public void insert(T key, String value) {
            int index = binarySearch(keys, key);
            if (index >= 0) {
                // Key already exists, update the value list
                List<String> valueList = values.get(index);
                int insertIndex = Collections.binarySearch(valueList, value);
                if (insertIndex < 0) {
                    insertIndex = -insertIndex - 1;
                }
                valueList.add(insertIndex, value);
            } else {
                // Key doesn't exist, insert it along with the value
                index = -index - 1;
                keys.add(index, key);
                List<String> valueList = new ArrayList<>();
                valueList.add(value);
                values.add(index, valueList);
            }
        }


        @Override
        public void delete(T key, String value) {
            int index = binarySearch(keys, key);
            if (index >= 0) {
                List<String> valueList = values.get(index);
                int valueIndex = valueList.indexOf(value);
                if (valueIndex >= 0) {
                    valueList.remove(valueIndex);
                    if (valueList.isEmpty()) {
                        keys.remove(index);
                        values.remove(index);
                    }
                }
            }
        }

        @Override
        public boolean isOverflow() {
            return keys.size() > 100000000;
        }

        @Override
        public boolean isLeaf() {
            return true;
        }

        @Override
        public void split(Node<T> parent, int index) {
            int mid = keys.size() / 2;
            LeafNode<T> newLeaf = new LeafNode<>();
            newLeaf.keys.addAll(keys.subList(mid, keys.size()));
            newLeaf.values.addAll(values.subList(mid, values.size()));
            keys.subList(mid, keys.size()).clear();
            values.subList(mid, values.size()).clear();

            // Insert the new leaf node into the parent node
            parent.insert(newLeaf.keys.get(0), null);
            ((InternalNode<T>) parent).addChild(index + 1, newLeaf);
        }




        @Override
        public int binarySearch(List<T> list, T key) {
            int low = 0;
            int high = list.size() - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                T midVal = list.get(mid);
                int cmp = midVal.compareTo(key);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid; // key found
                }
            }
            return -(low + 1);  // key not found
        }
    }

    private class InternalNode<T extends Comparable<T>> implements Node<T> {
        List<T> keys;
        List<Node<T>> children;

        InternalNode() {
            this.keys = new ArrayList<>();
            this.children = new ArrayList<>();
        }

        @Override
        public List<String> search(T key) {
            int index = binarySearch(keys, key);
            if (index >= 0) {
                return children.get(index + 1).search(key);
            } else {
                index = -index - 1;
                return children.get(index).search(key);
            }
        }

        @Override
        public void insert(T key, String value) {
            int index = binarySearch(keys, key);
            if (index >= 0) {
                children.get(index + 1).insert(key, value);
            } else {
                index = -index - 1;
                children.get(index).insert(key, value);
                if (children.get(index).isOverflow()) {
                    children.get(index).split(this, index);
                }
            }
        }

        @Override
        public void delete(T key, String value) {
            int index = binarySearch(keys, key);
            if (index >= 0) {
                children.get(index + 1).delete(key, value);
            } else {
                index = -index - 1;
                children.get(index).delete(key, value);
                if (((InternalNode<T>) children.get(index)).keys.isEmpty()) {
                    children.remove(index);
                    keys.remove(index);
                }
            }
        }

        @Override
        public boolean isOverflow() {
            return keys.size() > 10000000;
        }

        @Override
        public boolean isLeaf() {
            return false;
        }

        @Override
        public void split(Node<T> parent, int index) {
            int mid = keys.size() / 2;
            InternalNode<T> newInternal = new InternalNode<>();
            newInternal.keys.addAll(keys.subList(mid + 1, keys.size()));
            newInternal.children.addAll(children.subList(mid + 1, children.size()));
            keys.subList(mid, keys.size()).clear();
            children.subList(mid + 1, children.size()).clear();

            // Insert the new key into the parent node
            parent.insert(keys.get(mid), null);
            addChild(index + 1, newInternal);
        }



        @Override
        public int binarySearch(List<T> list, T key) {
            int low = 0;
            int high = list.size() - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                T midVal = list.get(mid);
                int cmp = midVal.compareTo(key);

                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return mid; // key found
                }
            }
            return -(low + 1);  // key not found
        }

        public void addChild(int index, Node<T> child) {
            children.add(index, child);
        }
        
    }
 // Method to serialize the BPlusTree to a file
 // Method to serialize the BPlusTree data to a file
    public void saveToFile(String fileName) {
        try (FileOutputStream fileOut = new FileOutputStream(fileName);
             ObjectOutputStream out = new ObjectOutputStream(fileOut)) {
            // Serialize only the necessary data (keys and values)
            Map<T, List<String>> data = new HashMap<>();
            traverse(root, data);
            out.writeObject(data);
            System.out.println("BPlusTree data saved to " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Static method to deserialize the BPlusTree data from a file
    public static <T extends Comparable<T>> BPlusTree<T> loadFromFile(String fileName, int degree, Class<?> keyType) {
        BPlusTree<T> bPlusTree = new BPlusTree<>(degree, keyType);
        try (FileInputStream fileIn = new FileInputStream(fileName);
             ObjectInputStream in = new ObjectInputStream(fileIn)) {
            // Deserialize the data
            Map<T, List<String>> data = (Map<T, List<String>>) in.readObject();
            // Reconstruct the tree
            for (Map.Entry<T, List<String>> entry : data.entrySet()) {
                for (String value : entry.getValue()) {
                    bPlusTree.insert(entry.getKey(), value);
                }
            }
            System.out.println("BPlusTree data loaded from " + fileName);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return bPlusTree;
    }

    // Helper method to traverse the tree and collect data
    private void traverse(Node<T> node, Map<T, List<String>> data) {
        if (node.isLeaf()) {
            LeafNode<T> leaf = (LeafNode<T>) node;
            for (int i = 0; i < leaf.keys.size(); i++) {
                data.put(leaf.keys.get(i), leaf.values.get(i));
            }
        } else {
            InternalNode<T> internal = (InternalNode<T>) node;
            for (int i = 0; i < internal.keys.size(); i++) {
                traverse(internal.children.get(i), data);
                if (i == internal.keys.size() - 1) {
                    traverse(internal.children.get(i + 1), data);
                }
            }
        }
    }

    public int size() {
        return countNodes(root);
    }

    private int countNodes(Node<T> node) {
        if (node == null) {
            return 0;
        }
        int count = 1; // Count the current node
        if (!node.isLeaf()) {
            InternalNode<T> internalNode = (InternalNode<T>) node;
            for (Node<T> child : internalNode.children) {
                count += countNodes(child); // Recursively count child nodes
            }
        }
        return count;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        toStringHelper(root, sb, 0);
        return sb.toString();
    }

    private void toStringHelper(Node<T> node, StringBuilder sb, int depth) {
        if (node != null) {
            if (!node.isLeaf()) {
                InternalNode<T> internalNode = (InternalNode<T>) node;
                for (int i = 0; i < internalNode.keys.size(); i++) {
                    toStringHelper(internalNode.children.get(i), sb, depth + 1);
                    for (int j = 0; j < depth; j++) {
                        sb.append("\t");
                    }
                    sb.append(internalNode.keys.get(i)).append(": ");
                    sb.append(internalNode.children.get(i + 1).search(internalNode.keys.get(i))).append("\n");
                }
                toStringHelper(internalNode.children.get(internalNode.children.size() - 1), sb, depth + 1);
            } else {
                LeafNode<T> leafNode = (LeafNode<T>) node;
                for (int j = 0; j < depth; j++) {
                    sb.append("\t");
                }
                sb.append("Leaf: ").append(leafNode.keys).append(" - ");
                sb.append(leafNode.values).append("\n");
            }
        }
    }

    public String findClosestValue(Object key, String line) {
        // Extract key and values from the line
        int startValuesIndex = line.indexOf("[[");
        List<String> keyPart = new ArrayList<>();
        int startKeys = line.indexOf("[") + 1;
        int endKeys = line.indexOf("]");
        String[] keysArray = line.substring(startKeys, endKeys).split(", ");
        for (String keyString : keysArray) {
            keyPart.add(keyString);
        }
        List<String> keys = new ArrayList<>();
        int startValues = startValuesIndex + 2;
        int endValues = line.lastIndexOf("]]");
        String[] valueParts = line.substring(startValues, endValues).split("], \\[");
        for (String valuePart : valueParts) {
            String[] pair = valuePart.split(", ");
            keys.add(pair[0]);
        }
        for(int h = 0; h<valueParts.length;h++){
        System.out.println(valueParts[h]);
        }
        // Convert key to appropriate type for comparison
        //Object convertedKey;
        if (key instanceof String) {
             String convertedKey = (String) key;
             // Find the closest value based on the key
             if (keys.isEmpty()) {
                 throw new IllegalArgumentException("No keys found in the line.");
             }

             String firstKey = keysArray[0];
             String lastKey = keysArray[keysArray.length - 1];

             if ((convertedKey).compareTo(firstKey) < 0) {
                 return "First";
             } else if (((Comparable<T>) convertedKey).compareTo((T) lastKey) > 0) {
                 return "Last";
             } else {
                 String before = "";
                 String after = "";
                 String bValues = "";
                 String aValues = "";
                 for (int i = 0; i < keysArray.length - 1; i++) {
                     String currentKey = keysArray[i];
                     String nextKey = keysArray[i + 1];
                     String bv = valueParts[i];
                     String av = valueParts[i+1];
                     String convertedCurrentKey =  currentKey;
                     String convertedNextKey = nextKey;
                     if (((convertedKey).compareTo(convertedCurrentKey) > 0 && ( convertedKey).compareTo(convertedNextKey) < 0) || ( convertedKey).compareTo(convertedCurrentKey) == 0) {
                     	before = currentKey;
                         after = nextKey;
                         bValues = bv;
                         aValues = av;
                         break;
                     }
                 }
                 if (before.isEmpty() && after.isEmpty()) {
                     return "Last"; // If no match is found, key is greater than all keys
                 } else {
                     return "After " + before +" "+ bValues;
                 }
             }
        } else if (key instanceof Integer) {
             String convertedKey =  key.toString();
             // Find the closest value based on the key
             if (keys.isEmpty()) {
                 throw new IllegalArgumentException("No keys found in the line.");
             }

             String firstKey = keysArray[0];
             String lastKey = keysArray[keysArray.length - 1];

             if ((convertedKey).compareTo(firstKey) < 0) {
                 return "First";
             } else if ((convertedKey).compareTo(lastKey) > 0) {
                 return "Last";
             } else {
                 String before = "";
                 String after = "";
                 String bValues = "";
                 String aValues = "";
                 for (int i = 0; i < keysArray.length - 1; i++) {
                     String currentKey = keysArray[i];
                     String nextKey = keysArray[i + 1];
                     String bv = valueParts[i];
                     String av = valueParts[i+1];
                     String convertedCurrentKey;
                     convertedCurrentKey = currentKey.toString();
                     String convertedNextKey;
                     convertedNextKey = nextKey.toString();

                     if (((convertedKey).compareTo(convertedCurrentKey) > 0 && (convertedKey).compareTo(convertedNextKey) < 0) || (convertedKey).compareTo(convertedCurrentKey) == 0) {
                     	before = currentKey;
                         after = nextKey;
                         bValues = bv;
                         aValues = av;
                         break;
                     }
                 }
                 if (before.isEmpty() && after.isEmpty()) {
                     return "Last"; // If no match is found, key is greater than all keys
                 } else {
                     return "After " + before +" "+ bValues;
                 }
             }
        } else if (key instanceof Double) {
        	String convertedKey = key.toString();
             // Find the closest value based on the key
             if (keys.isEmpty()) {
                 throw new IllegalArgumentException("No keys found in the line.");
             }

             String firstKey = keysArray[0];
             String lastKey = keysArray[keysArray.length - 1];

             if ((convertedKey).compareTo(firstKey) < 0) {
                 return "First";
             } else if ((convertedKey).compareTo(lastKey) > 0) {
                 return "Last";
             } else {
                 String before = "";
                 String after = "";
                 String bValues = "";
                 String aValues = "";
                 for (int i = 0; i < keysArray.length - 1; i++) {
                     String currentKey = keysArray[i];
                     String nextKey = keysArray[i + 1];
                     String bv = valueParts[i];
                     String av = valueParts[i+1];
                     String convertedCurrentKey;
                     convertedCurrentKey = currentKey;
                     String convertedNextKey;
                     convertedNextKey = nextKey;
                     if (((convertedKey).compareTo(convertedCurrentKey) > 0 && (convertedKey).compareTo(convertedNextKey) < 0) || (convertedKey).compareTo(convertedCurrentKey) == 0) {
                     	before = currentKey;
                         after = nextKey;
                         bValues = bv;
                         aValues = av;
                         break;
                     }
                 }
                 if (before.isEmpty() && after.isEmpty()) {
                     return "Last"; // If no match is found, key is greater than all keys
                 } else {
                     return "After " + before +" "+ bValues;
                 }
             }
        } else {
            throw new IllegalArgumentException("Unsupported key type.");
        }


    }

    public static int extractPageNumber(String input) {
        // Define a regular expression pattern to match the last number in the string
        Pattern pattern = Pattern.compile("\\d+$");
        Matcher matcher = pattern.matcher(input);

        // Find the last occurrence of a number in the string
        if (matcher.find()) {
            String pageNumberString = matcher.group();
            // Convert the matched string to an integer
            return Integer.parseInt(pageNumberString);
        } else {
            // If no number is found, return -1 or throw an exception based on your requirements
            return -1; // Or throw an exception indicating no number found
        }
    }
    
    public void deleteFromIndex(T key, String value) throws IllegalArgumentException {
        boolean deleted = deleteFromIndex(root, key, value);
        if (!deleted) {
            throw new IllegalArgumentException("Key or value not found in the index.");
        }
        saveToFile("index.ser");
    }

    private boolean deleteFromIndex(Node<T> node, T key, String value) {
        if (node != null) {
            if (node.isLeaf()) {
                LeafNode<T> leaf = (LeafNode<T>) node;
                int index = leaf.keys.indexOf(key);
                if (index >= 0) {
                    List<String> valueList = leaf.values.get(index);
                    if (valueList.remove(value)) {
                        if (valueList.isEmpty()) {
                            // If value list is empty after removal, remove the key as well
                            leaf.keys.remove(index);
                            leaf.values.remove(index);
                        }
                        return true;
                    }
                }
                return false; // Key or value not found
            } else {
                InternalNode<T> internal = (InternalNode<T>) node;
                int index = internal.binarySearch(internal.keys, key);
                if (index >= 0) {
                    return deleteFromIndex(internal.children.get(index + 1), key, value);
                } else {
                    index = -index - 1;
                    return deleteFromIndex(internal.children.get(index), key, value);
                }
            }
        }
        return false; // Key or value not found
    }


    
    public static void main(String[] args) {
    	
        // Load the String BPlusTree from file
    	BPlusTree<String> loadedStringBPlusTree = new BPlusTree<String>(3, String.class);
        System.out.println(loadedStringBPlusTree.toString());
        // Insert values into the loaded BPlusTree
        loadedStringBPlusTree.insert("apple1", "page11");
        loadedStringBPlusTree.insert("apple1", "page11");
        loadedStringBPlusTree.insert("apple2", "page21");
        loadedStringBPlusTree.insert("apple3", "page31");
        loadedStringBPlusTree.insert("apple1", "page12");

        System.out.println(loadedStringBPlusTree.toString());
        loadedStringBPlusTree.saveToFile("applebplustree.ser");
        loadedStringBPlusTree = BPlusTree.loadFromFile("applebplustree.ser", 3, String.class);
        System.out.println(loadedStringBPlusTree.toString());
        loadedStringBPlusTree.deleteFromIndex("apple1", "page11");
        loadedStringBPlusTree.deleteFromIndex("apple3", "page31");
        loadedStringBPlusTree.saveToFile("applebplustree.ser");
    	loadedStringBPlusTree = BPlusTree.loadFromFile("applebplustree.ser", 3, String.class);
    	System.out.println(loadedStringBPlusTree.toString());

    }


        }
    
