import { configureStore } from '@reduxjs/toolkit'
import appReducer from '../features/appSlice'
const store=configureStore({
    reducer:{
        app:appReducer
    }
})

export default store;