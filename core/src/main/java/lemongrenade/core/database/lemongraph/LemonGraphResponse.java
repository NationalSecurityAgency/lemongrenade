package lemongrenade.core.database.lemongraph;

import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpFields;
import org.json.JSONObject;

/**
 * Dealing with common result variables from LemonGraph Communication
 * LemonGraph sends result variables in Response headers for Posts.
 *
 * jobId is only populated on certain calls, (create) so don't rely on it for other calls
 */
public class LemonGraphResponse {
    private String jobId;
    private int maxId;        // X-lg-maxID
    private int currentId;    // X-lg-currentID
    private int updateCount;  // X-lg-updates Number of updates in last post
    private int responseCode;
    private JSONObject content;
    private boolean success;
    public String getJobId() { return jobId; }
    public void setJobId(String j) { jobId = j; }
    public int getCurrentId() { return currentId;}
    public void setCurrentId(int currentId) { this.currentId = currentId; }
    public int  getMaxId()  { return maxId; }
    public void setMaxId(int maxId){ this.maxId = maxId; }
    public int  getUpdateCount() { return updateCount; }
    public void setUpdateCount(int updateCount) { this.updateCount = updateCount; }
    public int  getResponseCode() { return responseCode; }
    public void setResponseCode(int responseCode) { this.responseCode = responseCode; }
    public JSONObject getContent() { return content; }
    public void setContent(JSONObject content) { this.content = content; }
    public void setSuccess(boolean success) { this.success = success;}
    public boolean getSuccess() { return this.success; }

    public LemonGraphResponse() {
        this.jobId = "";
        this.maxId = 0;
        this.currentId = 0;
        this.updateCount = 0;
        this.responseCode = 0;
        this.content = new JSONObject();
        this.success = false;
    }

    public void parseJobId(JSONObject body) {
        if (body.has("uuid")) {
            setJobId(body.getString("uuid"));
            setSuccess(true);
        }
    }

    /**
     * Call this with res.getHeaders();
     * @param headers
     */
    public void parseHeadersAndSetVariables(HttpFields headers) {
        for (HttpField h : headers) {
            if (h.getName().equalsIgnoreCase("X-lg-maxID")) {
                this.maxId = h.getIntValue();
            }
            if (h.getName().equalsIgnoreCase("X-lg-currentID")) {
                this.currentId = h.getIntValue();
            }
            else if (h.getName().equalsIgnoreCase("X-lg-updates")) {
                this.updateCount = h.getIntValue();
            }
        }
    }

    /** Parse out information from LemonGraph response */
    public void parseContentResponse(ContentResponse res) {
        JSONObject ret = new JSONObject();
        try {
            ret = new JSONObject(res.getContentAsString());
        }
        catch (Exception e) {}
        parseHeadersAndSetVariables(res.getHeaders());
        parseJobId(ret);
        setContent(ret);
        setResponseCode(res.getStatus());
        setSuccess(true);
    }

    public boolean didCallSucceed() { return success; }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Response code:").append(responseCode);
        sb.append("  Jobid  : ").append(jobId);
        sb.append("  MaxId  : ").append(maxId);
        sb.append("  CurrentId:").append(currentId);
        sb.append("  Update count: ").append(updateCount);
        sb.append("  Success: ").append(success);
        sb.append("  Content: ").append(content);
        return sb.toString();
    }
}