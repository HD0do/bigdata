package com.atguigu.bean;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Getter
@Setter
@Data
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
