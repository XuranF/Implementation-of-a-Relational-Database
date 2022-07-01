package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    final static boolean[][] compatibilityMatrix={{true,true,true,true,true,true},//Nl
                                            {true,true,true,true,true,false},//IS
                                            {true,true,true,false,false,false},//IX
                                            {true,true,false,true,false,false},//S
                                            {true,true,false,false,false,false},//SIX
                                            {true,false,false,false,false,false}};//X
    final static boolean[][] parentMatrix={{true,false,false,false,false,false},
                                     {true,true,false,true,false,false},
                                     {true,true,true,true,true,true},
                                     {true,false,false,false,false,false},
                                     {true,false,true,false,true,true},
                                     {true,false,false,false,false,false}};
    final static boolean[][] substituteMatrix={{true,false,false,false,false,false},
                                         {true,true,false,false,false,false},
                                         {true,true,true,false,false,false},
                                         {true,true,false,true,false,false},
                                         {true,true,true,true,true,false},
                                         {true,true,true,true,true,true}};
    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return compatibilityMatrix[matrixIndex(a)][matrixIndex(b)];
    }
    private static int matrixIndex(LockType l){
        switch (l){
            case S: return 3;
            case X: return 5;
            case IS: return 1;
            case IX: return 2;
            case SIX: return 4;
            case NL: return 0;
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return parentMatrix[matrixIndex(parentLockType)][matrixIndex(childLockType)];
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return substituteMatrix[matrixIndex(substitute)][matrixIndex(required)];
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

