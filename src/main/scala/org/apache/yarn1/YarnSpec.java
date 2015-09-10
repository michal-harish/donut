package org.apache.yarn1;


import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

public class YarnSpec {
    private Resource capability;
    private Priority priority;
    private Class<?> mainClass;

    public YarnSpec(Resource capability, Priority priority,
            Class<?> mainClass) {
        this.capability = capability;
        this.priority = priority;
        this.mainClass = mainClass;
    }

    @Override
    public String toString() {
        return "priority " + priority.getPriority() + ", "+ capability.getMemory() +"Mb, " + capability.getVirtualCores() + "vcores"; 
    }

    @Override
    public int hashCode() {
        return capability.hashCode() + priority.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof YarnSpec) {
            return ((YarnSpec) obj).capability.getMemory() == capability.getMemory()
                    && ((YarnSpec) obj).capability.getVirtualCores() == capability.getVirtualCores()
                    && ((YarnSpec) obj).priority.equals(priority)
                    && ((YarnSpec) obj).mainClass.equals(mainClass)
                    ;
        } else {
            return false;
        }
    }

    public Class<?> getMainClass() {
        return mainClass;
    }

    public boolean isSatisfiedBy(Container container) {
        return container.getResource().getMemory() >= capability.getMemory()
                && container.getResource().getVirtualCores() >= capability.getVirtualCores()
                && container.getPriority().getPriority() >= priority.getPriority();
    }

    public Resource getCapability() {
        return capability;
    }

    public Priority getPriority() {
        return priority;
    }
}
