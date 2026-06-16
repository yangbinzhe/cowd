<script setup lang="ts">
import MarkdownIt from 'markdown-it';
import { computed } from 'vue';

const props = defineProps<{ content: string }>();
const md = new MarkdownIt({ html: false, linkify: true, breaks: true });
const html = computed(() => md.render(props.content || ''));
</script>

<template>
  <div class="markdown-body" v-html="html" />
</template>
