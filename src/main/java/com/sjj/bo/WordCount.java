package com.sjj.bo;

import lombok.Getter;
import lombok.Setter;

/**
 * 单词统计的bean
 * @author Tim
 *
 */
@Setter
@Getter
public class WordCount {
	private String word;
	private Integer count;
}
