<script setup lang="ts">
import { computed } from 'vue';
import VChart from 'vue-echarts';
import { use } from 'echarts/core';
import { BarChart, GraphChart, HeatmapChart, LineChart, PieChart, RadarChart } from 'echarts/charts';
import { GridComponent, LegendComponent, RadarComponent, TooltipComponent, VisualMapComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import type { ChartPoint } from '../types';

use([CanvasRenderer, LineChart, BarChart, PieChart, RadarChart, HeatmapChart, GraphChart, GridComponent, TooltipComponent, LegendComponent, RadarComponent, VisualMapComponent]);

const props = defineProps<{
  title: string;
  kind: 'line' | 'bar' | 'donut' | 'radar' | 'heatmap' | 'graph';
  data: ChartPoint[];
}>();

const option = computed(() => {
  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text').trim() || '#ecebe4';
  const muted = getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim() || '#a7a79d';
  const accent = getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#d7d6ce';
  if (props.kind === 'donut') {
    return { tooltip: { trigger: 'item' }, legend: { bottom: 0, textStyle: { color: muted } }, series: [{ type: 'pie', radius: ['48%', '72%'], data: props.data, label: { color: textColor } }] };
  }
  if (props.kind === 'radar') {
    return { tooltip: {}, radar: { indicator: props.data.map((d) => ({ name: d.name, max: 100 })), axisName: { color: muted } }, series: [{ type: 'radar', areaStyle: { opacity: 0.18 }, data: [{ value: props.data.map((d) => d.value), name: props.title }] }] };
  }
  if (props.kind === 'heatmap') {
    return { tooltip: {}, grid: { left: 20, right: 20, top: 20, bottom: 30 }, xAxis: { type: 'category', data: props.data.map((d) => d.name), axisLabel: { color: muted } }, yAxis: { type: 'category', data: ['Usage'], axisLabel: { color: muted } }, visualMap: { show: false, min: 0, max: 60 }, series: [{ type: 'heatmap', data: props.data.map((d, i) => [i, 0, d.value]), label: { show: true, color: textColor } }] };
  }
  if (props.kind === 'graph') {
    return { tooltip: {}, series: [{ type: 'graph', layout: 'force', roam: false, force: { repulsion: 120 }, data: props.data.map((d, i) => ({ name: d.name, value: d.value, symbolSize: 34 + i * 8 })), links: props.data.slice(1).map((d) => ({ source: props.data[0].name, target: d.name })), label: { show: true, color: textColor }, lineStyle: { color: accent } }] };
  }
  return { tooltip: { trigger: 'axis' }, grid: { left: 32, right: 18, top: 24, bottom: 28 }, xAxis: { type: 'category', data: props.data.map((d) => d.name), axisLabel: { color: muted } }, yAxis: { type: 'value', axisLabel: { color: muted }, splitLine: { lineStyle: { color: 'rgba(128,128,128,.16)' } } }, series: [{ type: props.kind, data: props.data.map((d) => d.value), smooth: props.kind === 'line', itemStyle: { color: accent }, areaStyle: props.kind === 'line' ? { opacity: 0.08 } : undefined }] };
});
</script>

<template>
  <section class="chart-panel">
    <header>
      <h2>{{ title }}</h2>
      <span>{{ kind }}</span>
    </header>
    <VChart class="chart" :option="option" autoresize />
  </section>
</template>
