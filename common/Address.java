package au.edu.rmit.common;

import java.io.Serializable;

/**
 * address to locate a specific record
 */
public class Address implements Serializable {

    private static final long serialVersionUID = -2813868270123623482L;

    /**
     * page number where a record is located, based from 0
     */
    private int pageNo;

    /**
     * record number in its page, based from 0
     */
    private int recordNumber;

    public Address(int pageNo, int recordNumber) {
        this.pageNo = pageNo;
        this.recordNumber = recordNumber;
    }

    public int getPageNo() {
        return pageNo;
    }

    public int getRecordNumber() {
        return recordNumber;
    }

}
