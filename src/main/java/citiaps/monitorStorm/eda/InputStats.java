package citiaps.monitorStorm.eda;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class InputStats {
	private Map<String, Long> input;

	public InputStats() {
		this.setInput(new HashMap<String, Long>());
	}

	public long getStreamInput(String spout) {
		return this.input.get(spout);
	}

	public void setStreamInput(String spout, long input) {
		this.input.put(spout, input);
	}

	public Set<String> keyInput() {
		return this.input.keySet();
	}

	public Map<String, Long> getInput() {
		return input;
	}

	public void setInput(Map<String, Long> input) {
		this.input = input;
	}

	public void clear() {
		for (String key : keyInput()) {
			this.input.put(key, Long.valueOf(0));
		}
	}

}
