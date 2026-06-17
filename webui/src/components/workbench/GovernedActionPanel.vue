<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import ExecutionModeSwitch from './ExecutionModeSwitch.vue';
import ImpactPreview from './ImpactPreview.vue';
import PayloadEditor from './PayloadEditor.vue';
import SchemaForm from './SchemaForm.vue';
import RequestReceipt from './RequestReceipt.vue';

type MfgWriteContract = {
  id: string;
  domain: string;
  title: string;
  endpoint: string;
  method: string;
  summary: string;
  current_return: string;
  validate?: string;
  plan?: string;
  dry_run?: string;
  live?: boolean;
  live_policy?: string;
  receipt?: boolean;
  audit_ref?: boolean;
  changed_refs?: boolean;
  approval_required?: boolean;
  kernel_boundary?: string;
  affected_refs?: string[];
  fields?: Array<{ name: string; label?: string; required?: boolean; type?: string; options?: string[] }>;
};

const props = defineProps<{
  contract: MfgWriteContract;
  payload?: Record<string, unknown>;
  receipt?: unknown;
}>();

const emit = defineEmits<{
  plan: [payload: Record<string, unknown>];
  dryRun: [payload: Record<string, unknown>];
  live: [payload: Record<string, unknown>];
}>();

const mode = ref('plan');
const payloadDraft = ref<Record<string, unknown>>({ ...(props.payload || {}) });

watch(() => props.payload, (value) => {
  payloadDraft.value = { ...(value || {}) };
}, { deep: true });

const capabilityRows = computed(() => [
  ['validate', props.contract.validate || 'not declared'],
  ['plan', props.contract.plan || 'not declared'],
  ['dry-run', props.contract.dry_run || 'not declared'],
  ['live', props.contract.live ? props.contract.live_policy || 'allowed' : 'unsupported'],
  ['receipt', props.contract.receipt ? 'required' : 'missing'],
  ['audit', props.contract.audit_ref ? 'required' : 'missing'],
  ['changed refs', props.contract.changed_refs ? 'required' : 'missing'],
]);

function run() {
  if (mode.value === 'dry_run') emit('dryRun', payloadDraft.value);
  else if (mode.value === 'live') emit('live', payloadDraft.value);
  else emit('plan', payloadDraft.value);
}
</script>

<template>
  <section class="governed-action-panel" :data-contract-id="contract.id" :data-domain="contract.domain">
    <header>
      <div>
        <span>{{ contract.domain }}</span>
        <h3>{{ contract.title }}</h3>
      </div>
      <code>{{ contract.method }} {{ contract.endpoint }}</code>
    </header>
    <p>{{ contract.summary }}</p>
    <SchemaForm v-model="payloadDraft" :fields="contract.fields" />
    <PayloadEditor v-model="payloadDraft" />
    <ExecutionModeSwitch v-model="mode" :live-policy="contract.live_policy" />
    <ImpactPreview
      :refs="contract.affected_refs"
      :boundary="contract.kernel_boundary"
      :approval-required="contract.approval_required"
    />
    <dl class="contract-list">
      <template v-for="row in capabilityRows" :key="row[0]">
        <dt>{{ row[0] }}</dt>
        <dd>{{ row[1] }}</dd>
      </template>
      <dt>return</dt>
      <dd>{{ contract.current_return }}</dd>
    </dl>
    <div class="button-row">
      <button class="primary-action" type="button" @click="run">Run {{ mode }}</button>
    </div>
    <RequestReceipt :receipt="receipt" :title="`${contract.title} receipt`" />
  </section>
</template>
