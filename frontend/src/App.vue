<script setup>
import { ref, computed, onUnmounted } from "vue";

const URL_API = `${import.meta.env.VITE_API}`;

const dataTransmission = ref(false);
const intervalMany = ref(null);
const intervalOne = ref(null);
const manyBuffer = ref([]);

const nameStatus = computed(() =>
  dataTransmission.value
    ? "Сообщения отправляются"
    : "Сообщения не отправляютя",
);
const nameButtonTransmission = computed(() =>
  dataTransmission.value
    ? "Остановить передачу сообщений"
    : "Включить передачу сообщений",
);

onUnmounted(() => {
  if (intervalMany.value) clearInterval(intervalMany.value);
  if (intervalOne.value) clearInterval(intervalOne.value);
});

function setStatusTransmission() {
  dataTransmission.value = !dataTransmission.value;
  if (dataTransmission.value) {
    intervalMany.value = setInterval(() => {
      sendManyMessage();
    }, 1000);
    intervalOne.value = setInterval(() => {
      sendOneMessage();
    }, 10000);
    sendManyMessage();
    sendOneMessage();
  } else {
    if (intervalMany.value) {
      clearInterval(intervalMany.value);
      intervalMany.value = null;
    }
    if (intervalOne.value) {
      clearInterval(intervalOne.value);
      intervalOne.value = null;
    }
  }
}

function sendManyMessage() {
  const indexInBatch = manyBuffer.value.length + 1; // 1..10
  manyBuffer.value.push({
    key: "many",
    msg: `Пакетное сообщение номер ${indexInBatch}`,
  });

  if (manyBuffer.value.length === 10) {
    const batch = [...manyBuffer.value];
    manyBuffer.value = [];
    fetch(`${URL_API}/messages/batch`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(batch),
    })
      .then((response) => {
        if (response.ok) {
          return response.json().then((res) => {
            console.log(`Пакет из ${batch.length} сообщений отправлен!`);
            console.log(res.msg);
          });
        } else {
          console.error("Ошибка отправки пакетных сообщений:", response.status);
        }
      })
      .catch((err) => {
        console.error("Ошибка сети (batch):", err);
      });
  }
}

function sendOneMessage() {
  const currentDate = new Date();
  const isoDate = currentDate.toISOString();
  try {
    fetch(`${URL_API}/message`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ key: "one", msg: `${isoDate}: Одиночное сообщение` }),
    })
      .then((response) => {
        if (response.ok) {
          return response.json().then((res) => {
            console.log("Cooбщение отправлено!");
            console.log(res.msg);
          });
        } else {
          console.error("Ошибка отправки сообщения: ", response.status);
        }
      })
      .catch((err) => {
        console.error("Ошибка сети: ", err);
      });
  } catch (error) {
    console.error("Ошибка в sendOneMessage(): ", error);
  }
}
</script>

<template>
  <div>
    <div>
      <p>Практическая работа №5</p>
      <p>
        Состояние отправки сообщений:
        <span
          :class="{
            'status-transmission-active': dataTransmission === true,
            'status-transmission-unactive': dataTransmission === false,
          }"
        >
          {{ nameStatus }}
        </span>
      </p>
      <button @click="setStatusTransmission">
        {{ nameButtonTransmission }}
      </button>
    </div>
  </div>
</template>

<style scoped>
.status-test-transmission-active,
.status-transmission-active {
  color: green;
}

.status-test-transmission-unactive,
.status-transmission-unactive {
  color: red;
}

button {
  margin-top: 10px;
  padding: 8px 16px;
  cursor: pointer;
}
</style>
