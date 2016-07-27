package lemongrenade.core.models;

import org.json.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LGJobHistory {
    public final static int LGHISTORY_TYPE_TASK = 1;
    public final static int LGHISTORY_TYPE_COMMAND = 2;
    private String command;
    private int commandType;
    private long startTime;
    private long endTime;
    private int graphChanges;
    private int graphMaxId;
    private int currentId;
    private int numberOfNewTasksGenerated;
    private String message;
    private String taskId;

    public LGJobHistory() {
    }

    public LGJobHistory(int commandType,
                        String command,
                        String taskId,
                        String message,
                        long startTime, long endTime,
                        int graphChanges,
                        int graphMaxId,
                        int numberOfNewTasksGenerated,
                        int currentId) {
        this.commandType = commandType;
        this.command = command;
        this.taskId = taskId;
        this.message = message;
        this.startTime = startTime;
        this.endTime = endTime;
        this.graphChanges = graphChanges;
        this.graphMaxId = graphMaxId;
        this.numberOfNewTasksGenerated = numberOfNewTasksGenerated;
        this.currentId = currentId;
    }

    public void setCurrentId(int currentId) { this.currentId = currentId; }
    public int getCurrentId() { return this.currentId; }
    public String getTaskId() { return this.taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public int getCommandType() { return this.commandType; }
    public void setCommandType(int commandType) { this.commandType = commandType; }
    public String getCommand() { return this.command; }
    public void setCommand(String command) { this.command = command; }
    public String getMessage() { return this.message; }
    public void setMessage(String message) { this.message = message; }
    public long getStartTime() { return this.startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public long getEndTime() { return this.endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }
    public int getGraphChanges() { return this.graphChanges; }
    public void setGraphChanges(int graphChanges) { this.graphChanges = graphChanges; }
    public int getGraphMaxId() { return this.graphMaxId; }
    public void setGraphMaxId(int graphMaxId) { this.graphMaxId = graphMaxId; }
    public int getNumberOfNewTasksGenerated() { return this.numberOfNewTasksGenerated; }
    public void setNumberOfNewTasksGenerated(int numberOfNewTasksGenerated) {
        this.numberOfNewTasksGenerated = numberOfNewTasksGenerated;
    }

    /** */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        Date startDate = new Date(this.startTime);
        Date endDate = new Date(this.endTime);
        sb.append("[" + sdf.format(startDate) + " to " + sdf.format(endDate) + " "
                + "taskid:"+taskId
                + " cmd:"+ this.command + " graphchanges:"
                + this.graphChanges + " graphMaxId:" + this.graphMaxId
                +" currentId:"+this.currentId
                + " numberOfGraphChanges:"  + this.numberOfNewTasksGenerated + "]\n");
        return sb.toString();
    }

    /** */
    public JSONObject toJson() {
        JSONObject jo = new JSONObject(1);
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss");
        Date startDate = new Date(this.startTime);
        Date endDate = new Date(this.endTime);
        jo.put("task_id", this.taskId);
        jo.put("commandtype", this.commandType);
        jo.put("message", this.message);
        jo.put("startdate", sdf.format(startDate));
        jo.put("enddate", sdf.format(endDate));
        jo.put("command", this.command);
        jo.put("graph_changes", this.graphChanges);
        jo.put("graph_max_id", this.graphMaxId);
        jo.put("current_id", this.currentId);
        jo.put("number_of_new_tasks_generated", this.numberOfNewTasksGenerated);
        return jo;
    }
}
