package com.sjj.bo;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OffsetInfo {
	private String topic;
	private Integer partition;
	private Long offset;
}
