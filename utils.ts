export const logInfo = (...args: any[]) => {
  console.log(new Date().toLocaleString(), '[INFO]', ...args);
};
